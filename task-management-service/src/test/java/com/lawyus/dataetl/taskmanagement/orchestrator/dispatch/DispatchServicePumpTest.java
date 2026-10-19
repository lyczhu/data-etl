package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorMetrics;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.DispatchProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.data.domain.Pageable;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DispatchServicePumpTest {

    private RedissonClient redisson;
    private EtlTaskRunRepository runRepository;
    private EtlTaskCheckpointRepository checkpointRepository;
    private OrchestratorService orchestrator;
    private TokenBucket tokenBucket;
    private ShardDispatchAssembler assembler;
    private DispatchPublisher publisher;
    private DispatchProperties dispatchProperties;
    private RoutingProperties routing;
    private OrchestratorMetrics metrics;
    private DispatchService service;

    @BeforeEach
    void setUp() {
        redisson = mock(RedissonClient.class);
        runRepository = mock(EtlTaskRunRepository.class);
        checkpointRepository = mock(EtlTaskCheckpointRepository.class);
        orchestrator = mock(OrchestratorService.class);
        tokenBucket = mock(TokenBucket.class);
        assembler = mock(ShardDispatchAssembler.class);
        publisher = mock(DispatchPublisher.class);
        dispatchProperties = new DispatchProperties();   // permitsPerRun=8, pumpBatch=50
        routing = new RoutingProperties();
        metrics = mock(OrchestratorMetrics.class);
        service = new DispatchService(redisson, runRepository, checkpointRepository, orchestrator,
                tokenBucket, assembler, publisher, dispatchProperties, routing, metrics, null);
    }

    private EtlTaskRun queuedRun() {
        EtlTaskRun run = EtlTaskRun.newRun("run-1", "task-1", 1);
        ReflectionTestUtils.setField(run, "status", "QUEUED");
        return run;
    }

    @Test
    @DisplayName("无 PENDING 分片：不申请令牌、不派工、不迁移")
    void noPendingShards() {
        when(checkpointRepository.countPendingShards("run-1")).thenReturn(0L);
        service.dispatchOne(queuedRun());
        verify(tokenBucket, never()).tryAcquire(anyInt());
        verify(publisher, never()).publish(any());
        verify(orchestrator, never()).transition(any(), any(), any(), any());
    }

    @Test
    @DisplayName("令牌充足：逐片派工 + PENDING→ASSIGNED + 首个分片触发 T5 QUEUED→DISPATCHED（仅一次）")
    void dispatchWithTokens() {
        EtlTaskRun run = queuedRun();
        when(checkpointRepository.countPendingShards("run-1")).thenReturn(2L);
        when(tokenBucket.tryAcquire(2)).thenReturn(true);
        EtlTaskCheckpoint s1 = EtlTaskCheckpoint.newPending("task-1", "run-1", "shard-1");
        EtlTaskCheckpoint s2 = EtlTaskCheckpoint.newPending("task-1", "run-1", "shard-2");
        when(checkpointRepository.findPendingShards(eq("run-1"), any(Pageable.class))).thenReturn(List.of(s1, s2));
        when(assembler.assemble(any(), any())).thenReturn(new ShardDispatchCommand());

        service.dispatchOne(run);

        verify(publisher, times(2)).publish(any(ShardDispatchCommand.class));
        verify(checkpointRepository).markAssigned("task-1", "run-1", "shard-1");
        verify(checkpointRepository).markAssigned("task-1", "run-1", "shard-2");
        verify(orchestrator, times(1))
                .transition(eq("run-1"), eq(EtlTaskStatus.QUEUED), eq(EtlTaskStatus.DISPATCHED), any(TransitionContext.class));
        verify(runRepository, never()).markQueuedReason(anyString(), anyString());
        verify(tokenBucket, never()).release(anyInt());   // 两片全派，无预留未用
    }

    @Test
    @DisplayName("令牌不足：标 queued_reason=THROTTLED（T6），不迁移、不派工")
    void throttledWhenNoTokens() {
        when(checkpointRepository.countPendingShards("run-1")).thenReturn(5L);
        when(tokenBucket.tryAcquire(anyInt())).thenReturn(false);

        service.dispatchOne(queuedRun());

        verify(runRepository).markQueuedReason("run-1", "THROTTLED");
        verify(metrics).dispatchThrottled();
        verify(publisher, never()).publish(any());
        verify(orchestrator, never()).transition(any(), any(), any(), any());
    }

    @Test
    @DisplayName("permits 按 min(pending, permitsPerRun) 申请，预占未派即时归还")
    void clampsPermitsAndReleasesUnused() {
        dispatchProperties.setPermitsPerRun(8);
        EtlTaskRun run = queuedRun();
        when(checkpointRepository.countPendingShards("run-1")).thenReturn(100L);
        when(tokenBucket.tryAcquire(8)).thenReturn(true);
        // 只返回 3 片（其余 PENDING 未取到），应归还 8-3=5 个预留
        when(checkpointRepository.findPendingShards(eq("run-1"), any(Pageable.class)))
                .thenReturn(List.of(
                        EtlTaskCheckpoint.newPending("task-1", "run-1", "s1"),
                        EtlTaskCheckpoint.newPending("task-1", "run-1", "s2"),
                        EtlTaskCheckpoint.newPending("task-1", "run-1", "s3")));
        when(assembler.assemble(any(), any())).thenReturn(new ShardDispatchCommand());

        service.dispatchOne(run);

        verify(tokenBucket).tryAcquire(8);
        verify(publisher, times(3)).publish(any());
        verify(tokenBucket).release(5);
    }

    @Test
    @DisplayName("影子门控关闭时 pump 直接返回（不查 QUEUED）")
    void pumpDormantWhenRoutingOff() {
        routing.setNewWorker(false);
        service.pump();
        verify(runRepository, never()).findQueuedForDispatch(any());
    }
}
