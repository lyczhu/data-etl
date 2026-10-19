package com.lawyus.dataetl.taskmanagement.orchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorMetrics;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.TokenBucket;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.DltMessage;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DltListenerTest {

    private EtlTaskRunRepository runRepository;
    private OrchestratorService orchestrator;
    private OrchestratorMetrics metrics;
    private TokenBucket tokenBucket;
    private DltListener listener;

    @BeforeEach
    void setUp() {
        runRepository = mock(EtlTaskRunRepository.class);
        orchestrator = mock(OrchestratorService.class);
        metrics = mock(OrchestratorMetrics.class);
        tokenBucket = mock(TokenBucket.class);
        listener = new DltListener(new ObjectMapper(), runRepository, orchestrator, metrics,
                new OrchestratorProperties(), new RoutingProperties(), tokenBucket);
    }

    private EtlTaskRun runWith(String status) {
        EtlTaskRun run = EtlTaskRun.newRun("run-1", "task-1", 1);
        ReflectionTestUtils.setField(run, "status", status);
        return run;
    }

    private DltMessage msg(String errorClass) {
        DltMessage m = new DltMessage();
        m.setTaskId("task-1");
        m.setRunId("run-1");
        m.setShardId("shard-1");
        m.setErrorClass(errorClass);
        return m;
    }

    @Test
    @DisplayName("行级毒（FATAL）→ RUNNING→NEEDS_ATTENTION（T13）+ 计指标 + 归还令牌")
    void fatalTriggersNeedsAttention() {
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(runWith("RUNNING")));

        listener.handle(msg("FATAL"));

        verify(metrics).dltReceived();
        verify(tokenBucket).releaseWithinCapacity(1);
        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.RUNNING),
                eq(EtlTaskStatus.NEEDS_ATTENTION), any(TransitionContext.class));
    }

    @Test
    @DisplayName("非法迁移类 DLT → 仅告警，不动状态、不归还令牌（§7.6）")
    void illegalTransitionDoesNotTouchState() {
        listener.handle(msg("ILLEGAL_TRANSITION"));

        verify(metrics).dltReceived();
        verify(tokenBucket, never()).releaseWithinCapacity(any(Integer.class));
        verify(orchestrator, never()).transition(any(), any(), any(), any());
        verify(runRepository, never()).findByRunId(any());
    }

    @Test
    @DisplayName("终态 run 的 DLT → 幂等忽略，不迁移")
    void terminalRunIgnored() {
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(runWith("FAILED")));

        listener.handle(msg("FATAL"));

        verify(orchestrator, never()).transition(any(), any(), any(), any());
    }
}
