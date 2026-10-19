package com.lawyus.dataetl.taskmanagement.orchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.ConvergenceService;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.RunAggregate;
import com.lawyus.dataetl.taskmanagement.orchestrator.RunCounterProjector;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.TokenBucket;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.ResultMessage;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskDltRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ResultsListenerTest {

    private EtlTaskRunRepository runRepository;
    private EtlTaskCheckpointRepository checkpointRepository;
    private OrchestratorService orchestrator;
    private TokenBucket tokenBucket;
    private RunCounterProjector counterProjector;
    private ResultsListener listener;

    @BeforeEach
    void setUp() {
        runRepository = mock(EtlTaskRunRepository.class);
        checkpointRepository = mock(EtlTaskCheckpointRepository.class);
        orchestrator = mock(OrchestratorService.class);
        tokenBucket = mock(TokenBucket.class);
        counterProjector = mock(RunCounterProjector.class);
        OrchestratorProperties properties = new OrchestratorProperties();
        listener = new ResultsListener(new ObjectMapper(), runRepository, checkpointRepository,
                orchestrator, properties, new RoutingProperties(), tokenBucket, counterProjector,
                new ConvergenceService(orchestrator, properties, mock(EtlTaskDltRepository.class)));
    }

    private EtlTaskRun runWith(String status, int shardTotal) {
        EtlTaskRun run = EtlTaskRun.newRun("run-1", "task-1", 1);
        ReflectionTestUtils.setField(run, "status", status);
        ReflectionTestUtils.setField(run, "shardTotal", shardTotal);
        return run;
    }

    private static RunAggregate agg(long pending, long assigned, long running, long succeeded,
                                    long failed, long skipped, long rowsIn, long rowsOut,
                                    long rowsFiltered, long rowsFailed) {
        return new RunAggregate() {
            @Override public Long getPending() { return pending; }
            @Override public Long getAssigned() { return assigned; }
            @Override public Long getRunning() { return running; }
            @Override public Long getSucceeded() { return succeeded; }
            @Override public Long getFailed() { return failed; }
            @Override public Long getSkipped() { return skipped; }
            @Override public Long getRowsIn() { return rowsIn; }
            @Override public Long getRowsOut() { return rowsOut; }
            @Override public Long getRowsDropped() { return rowsFiltered + rowsFailed; }
            @Override public Long getRowsFiltered() { return rowsFiltered; }
            @Override public Long getRowsFailed() { return rowsFailed; }
            @Override public Long getRowsDeduped() { return 0L; }
        };
    }

    private ResultMessage msg() {
        ResultMessage m = new ResultMessage();
        m.setTaskId("task-1");
        m.setRunId("run-1");
        m.setShardId("shard-1");
        m.setStatus("SUCCEEDED");
        return m;
    }

    @Test
    @DisplayName("全成功且行数平 → RUNNING→SUCCEEDED（T10）+ 归还令牌")
    void convergesToSucceeded() {
        when(runRepository.findByRunId("run-1"))
                .thenReturn(Optional.of(runWith("RUNNING", 2)));
        when(checkpointRepository.aggregate("run-1"))
                .thenReturn(agg(0, 0, 0, 2, 0, 0, 100, 100, 0, 0));

        listener.handle(msg());

        verify(tokenBucket).releaseWithinCapacity(1);
        verify(counterProjector).project(eq("run-1"), any(RunAggregate.class));
        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.RUNNING),
                eq(EtlTaskStatus.SUCCEEDED), any(TransitionContext.class));
    }

    @Test
    @DisplayName("全成功 + filtered 行参与对账 → 仍 SUCCEEDED（WP-04）")
    void filteredRowsStillSucceed() {
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(runWith("RUNNING", 1)));
        when(checkpointRepository.aggregate("run-1"))
                .thenReturn(agg(0, 0, 0, 1, 0, 0, 10, 8, 2, 0));

        listener.handle(msg());

        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.RUNNING),
                eq(EtlTaskStatus.SUCCEEDED), any(TransitionContext.class));
    }

    @Test
    @DisplayName("全分片 SUCCEEDED 但存在 rows_failed → NEEDS_ATTENTION（不得静默成功）")
    void failedRowsNeverSucceed() {
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(runWith("RUNNING", 1)));
        when(checkpointRepository.aggregate("run-1"))
                .thenReturn(agg(0, 0, 0, 1, 0, 0, 10, 9, 0, 1));

        listener.handle(msg());

        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.RUNNING),
                eq(EtlTaskStatus.NEEDS_ATTENTION), any(TransitionContext.class));
        verify(orchestrator, never()).transition(eq("run-1"), eq(EtlTaskStatus.RUNNING),
                eq(EtlTaskStatus.SUCCEEDED), any(TransitionContext.class));
    }

    @Test
    @DisplayName("有失败其余成功 → RUNNING→PARTIAL_SUCCESS（T11）")
    void convergesToPartial() {
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(runWith("RUNNING", 2)));
        when(checkpointRepository.aggregate("run-1"))
                .thenReturn(agg(0, 0, 0, 1, 1, 0, 100, 50, 0, 0));

        listener.handle(msg());

        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.RUNNING),
                eq(EtlTaskStatus.PARTIAL_SUCCESS), any(TransitionContext.class));
    }

    @Test
    @DisplayName("全成功但行数不平 → RUNNING→NEEDS_ATTENTION(ROW_BALANCE_VIOLATION)（T13）")
    void imbalanceToNeedsAttention() {
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(runWith("RUNNING", 2)));
        when(checkpointRepository.aggregate("run-1"))
                .thenReturn(agg(0, 0, 0, 2, 0, 0, 100, 60, 10, 0));

        listener.handle(msg());

        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.RUNNING),
                eq(EtlTaskStatus.NEEDS_ATTENTION), any(TransitionContext.class));
    }

    @Test
    @DisplayName("仍有活跃分片 → 不收敛、不迁移，但计数仍投影（中间态不准是 bug）")
    void notConvergedNoTransition() {
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(runWith("RUNNING", 3)));
        when(checkpointRepository.aggregate("run-1"))
                .thenReturn(agg(0, 1, 1, 1, 0, 0, 50, 50, 0, 0));

        listener.handle(msg());

        verify(counterProjector).project(eq("run-1"), any(RunAggregate.class));
        verify(orchestrator, never()).transition(any(), any(), any(), any());
    }

    @Test
    @DisplayName("DISPATCHED 收到 results → 先补 T7 再收敛（两次迁移）")
    void dispatchBackfillsRunningThenConverges() {
        EtlTaskRun dispatched = runWith("DISPATCHED", 1);
        EtlTaskRun running = runWith("RUNNING", 1);
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(dispatched), Optional.of(running));
        when(checkpointRepository.aggregate("run-1"))
                .thenReturn(agg(0, 0, 0, 1, 0, 0, 10, 10, 0, 0));

        listener.handle(msg());

        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.DISPATCHED),
                eq(EtlTaskStatus.RUNNING), any(TransitionContext.class));
        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.RUNNING),
                eq(EtlTaskStatus.SUCCEEDED), any(TransitionContext.class));
    }

    @Test
    @DisplayName("终态 run 的重复 results → 幂等忽略（仅归还令牌，不投影不迁移）")
    void terminalRunIgnored() {
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(runWith("SUCCEEDED", 2)));

        listener.handle(msg());

        verify(orchestrator, never()).transition(any(), any(), any(), any());
        verify(counterProjector, never()).project(any(), any());
        verify(tokenBucket, times(1)).releaseWithinCapacity(anyInt());
    }
}
