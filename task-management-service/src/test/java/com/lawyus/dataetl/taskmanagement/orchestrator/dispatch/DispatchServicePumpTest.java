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
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.data.domain.Pageable;
import org.springframework.jdbc.core.JdbcTemplate;
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
    private JdbcTemplate jdbcTemplate;
    private DispatchService self;
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
        jdbcTemplate = mock(JdbcTemplate.class);
        self = mock(DispatchService.class);
        service = new DispatchService(redisson, runRepository, checkpointRepository, orchestrator,
                tokenBucket, assembler, publisher, dispatchProperties, routing, metrics, jdbcTemplate, self);
    }

    private EtlTaskRun run(String status) {
        EtlTaskRun run = EtlTaskRun.newRun("run-1", "task-1", 1);
        ReflectionTestUtils.setField(run, "status", status);
        return run;
    }

    private EtlTaskCheckpoint shard(String id) {
        return EtlTaskCheckpoint.newPending("task-1", "run-1", id);
    }

    private void allowDispatch(int permits, int pendingCount, int returnedShards) {
        when(checkpointRepository.countPendingShards("run-1")).thenReturn((long) pendingCount);
        when(tokenBucket.tryAcquireUpTo(anyInt())).thenReturn(permits);
        List<EtlTaskCheckpoint> shards = java.util.stream.IntStream.range(0, returnedShards)
                .mapToObj(i -> shard("shard-" + i)).toList();
        when(checkpointRepository.findPendingShards(eq("run-1"), any(Pageable.class))).thenReturn(shards);
        when(assembler.assemble(any(), any())).thenReturn(new ShardDispatchCommand());
        when(checkpointRepository.markAssigned(anyString(), anyString(), anyString(), anyInt())).thenReturn(1);
    }

    @Test
    @DisplayName("无 PENDING 分片：不申请令牌、不派工、不迁移")
    void noPendingShards() {
        when(checkpointRepository.countPendingShards("run-1")).thenReturn(0L);
        service.dispatchOne(run("QUEUED"));
        verify(tokenBucket, never()).tryAcquireUpTo(anyInt());
        verify(publisher, never()).publish(any());
        verify(orchestrator, never()).transition(any(), any(), any(), any());
    }

    @Test
    @DisplayName("令牌充足：逐片派工 + PENDING→ASSIGNED + 首个分片触发 T5 QUEUED→DISPATCHED（仅一次）")
    void dispatchWithTokens() {
        allowDispatch(2, 2, 2);

        service.dispatchOne(run("QUEUED"));

        verify(publisher, times(2)).publish(any(ShardDispatchCommand.class));
        verify(checkpointRepository).markAssigned("task-1", "run-1", "shard-0", 1);
        verify(checkpointRepository).markAssigned("task-1", "run-1", "shard-1", 1);
        verify(orchestrator, times(1))
                .transition(eq("run-1"), eq(EtlTaskStatus.QUEUED), eq(EtlTaskStatus.DISPATCHED), any(TransitionContext.class));
        verify(runRepository, never()).markQueuedReason(anyString(), anyString());
        verify(tokenBucket, never()).release(anyInt());
    }

    @Test
    @DisplayName("DISPATCHED/RUNNING 仍有 PENDING：持续补派，但不再触发 T5")
    void redispatchesForRunningRun() {
        allowDispatch(1, 1, 1);

        service.dispatchOne(run("DISPATCHED"));

        verify(publisher).publish(any(ShardDispatchCommand.class));
        verify(checkpointRepository).markAssigned("task-1", "run-1", "shard-0", 1);
        verify(orchestrator, never()).transition(any(), any(), any(), any());
    }

    @Test
    @DisplayName("令牌不足：标 queued_reason=THROTTLED（T6），不迁移、不派工")
    void throttledWhenNoTokens() {
        when(checkpointRepository.countPendingShards("run-1")).thenReturn(5L);
        when(tokenBucket.tryAcquireUpTo(5)).thenReturn(0);

        service.dispatchOne(run("QUEUED"));

        verify(runRepository).markQueuedReason("run-1", "THROTTLED");
        verify(metrics).dispatchThrottled();
        verify(publisher, never()).publish(any());
        verify(orchestrator, never()).transition(any(), any(), any(), any());
    }

    @Test
    @DisplayName("部分令牌：只派获得的许可数，未使用的许可即时归还")
    void dispatchesPartiallyWithLimitedTokens() {
        allowDispatch(3, 5, 3);

        service.dispatchOne(run("QUEUED"));

        verify(tokenBucket).tryAcquireUpTo(5);
        verify(publisher, times(3)).publish(any());
        verify(tokenBucket, never()).release(anyInt());
    }

    @Test
    @DisplayName("发送失败：分片保持 PENDING、归还全部预留许可、不触发 T5")
    void publishFailureKeepsPendingAndReleases() {
        when(checkpointRepository.countPendingShards("run-1")).thenReturn(2L);
        when(tokenBucket.tryAcquireUpTo(2)).thenReturn(2);
        when(checkpointRepository.findPendingShards(eq("run-1"), any(Pageable.class)))
                .thenReturn(List.of(shard("shard-0"), shard("shard-1")));
        when(assembler.assemble(any(), any())).thenReturn(new ShardDispatchCommand());
        org.mockito.Mockito.doThrow(new IllegalStateException("broker down"))
                .when(publisher).publish(any());

        service.dispatchOne(run("QUEUED"));

        verify(checkpointRepository, never()).markAssigned(anyString(), anyString(), anyString(), anyInt());
        verify(orchestrator, never()).transition(any(), any(), any(), any());
        verify(tokenBucket).release(2);
    }

    @Test
    @DisplayName("CAS 失败（并发泵已派）：归还该片令牌，不计派工")
    void casMissReleasesPermit() {
        when(checkpointRepository.countPendingShards("run-1")).thenReturn(1L);
        when(tokenBucket.tryAcquireUpTo(1)).thenReturn(1);
        when(checkpointRepository.findPendingShards(eq("run-1"), any(Pageable.class)))
                .thenReturn(List.of(shard("shard-0")));
        when(assembler.assemble(any(), any())).thenReturn(new ShardDispatchCommand());
        when(checkpointRepository.markAssigned(anyString(), anyString(), anyString(), anyInt())).thenReturn(0);

        service.dispatchOne(run("RUNNING"));

        verify(tokenBucket).release(1);
        verify(metrics, never()).dispatchShardPublished();
        verify(orchestrator, never()).transition(any(), any(), any(), any());
    }

    @Test
    @DisplayName("pump 扫描 QUEUED/DISPATCHED/RUNNING 的 run 并逐个补派")
    void pumpScansDispatchableRuns() {
        routing.setNewWorker(true);
        EtlTaskRun run = run("DISPATCHED");
        RLock lock = mock(RLock.class);
        when(redisson.getLock(anyString())).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        when(runRepository.findDispatchableRuns(any(Pageable.class))).thenReturn(List.of(run));

        service.pump();

        verify(self).dispatchOne(run);
        verify(lock).unlock();
    }

    @Test
    @DisplayName("影子门控关闭时 pump 直接返回（不查可派工 run）")
    void pumpDormantWhenRoutingOff() {
        routing.setNewWorker(false);
        service.pump();
        verify(runRepository, never()).findDispatchableRuns(any());
    }
}
