package com.lawyus.dataetl.taskmanagement.orchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorMetrics;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.ProgressMessage;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Date;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class ProgressListenerTest {

    private EtlTaskRunRepository runRepository;
    private EtlTaskCheckpointRepository checkpointRepository;
    private OrchestratorService orchestrator;
    private OrchestratorMetrics metrics;
    private ProgressListener listener;

    @BeforeEach
    void setUp() {
        runRepository = mock(EtlTaskRunRepository.class);
        checkpointRepository = mock(EtlTaskCheckpointRepository.class);
        orchestrator = mock(OrchestratorService.class);
        metrics = mock(OrchestratorMetrics.class);
        listener = new ProgressListener(new ObjectMapper(), runRepository, checkpointRepository,
                orchestrator, metrics, new OrchestratorProperties(), new RoutingProperties());
    }

    private EtlTaskRun runWith(String status) {
        EtlTaskRun run = EtlTaskRun.newRun("run-1", "task-1", 1);
        ReflectionTestUtils.setField(run, "status", status);
        return run;
    }

    private ProgressMessage msg() {
        ProgressMessage m = new ProgressMessage();
        m.setTaskId("task-1");
        m.setRunId("run-1");
        m.setShardId("shard-1");
        m.setTimestamp(1_700_000_000_000L);
        return m;
    }

    @Test
    @DisplayName("DISPATCHED 收到心跳 → T7 迁移 RUNNING + 单调 touch + checkpoint.touch")
    void firstHeartbeatMovesToRunning() {
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(runWith("DISPATCHED")));

        listener.handle(msg());

        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.DISPATCHED),
                eq(EtlTaskStatus.RUNNING), any(TransitionContext.class));
        verify(runRepository).touchProgress(eq("run-1"), any(Date.class));
        verify(checkpointRepository).touch(eq("run-1"), eq("shard-1"), any(Date.class));
    }

    @Test
    @DisplayName("RETRYING 窗口内活跃心跳 → 协议违例计数，不迁移")
    void protocolViolationInRetrying() {
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(runWith("RETRYING")));

        listener.handle(msg());

        verify(metrics).protocolViolation();
        verify(orchestrator, never()).transition(any(), any(), any(), any());
        verify(runRepository).touchProgress(eq("run-1"), any(Date.class));
    }

    @Test
    @DisplayName("RUNNING 心跳：不再触发 T7，仅刷新基线")
    void runningHeartbeatJustTouches() {
        when(runRepository.findByRunId("run-1")).thenReturn(Optional.of(runWith("RUNNING")));

        listener.handle(msg());

        verify(orchestrator, never()).transition(any(), any(), any(), any());
        verify(runRepository).touchProgress(eq("run-1"), any(Date.class));
    }

    @Test
    @DisplayName("run 不存在：无副作用")
    void runNotFoundIgnored() {
        when(runRepository.findByRunId(anyString())).thenReturn(Optional.empty());

        listener.handle(msg());

        verify(orchestrator, never()).transition(any(), any(), any(), any());
        verify(runRepository, never()).touchProgress(anyString(), any());
    }
}
