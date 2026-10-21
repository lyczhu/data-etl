package com.lawyus.dataetl.taskmanagement.kafka;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * 灰度路由隔离：{@code new-worker=true} 只对白名单 dataSource 生效；
 * 白名单外任务完成/失败仍必须走旧链路直写，不能因全局开关被误送状态机。
 */
class LegacyListenerRoutingTest {

    private static final long WHITELISTED_DS = 10L;
    private static final long LEGACY_DS = 8L;

    private EtlTaskRepository taskRepository;
    private EtlTaskService taskService;
    private OrchestratorService orchestrator;
    private RoutingProperties routing;
    private TaskCompletedListener completedListener;
    private TaskFailedListener failedListener;

    @BeforeEach
    void setUp() {
        taskRepository = mock(EtlTaskRepository.class);
        taskService = mock(EtlTaskService.class);
        orchestrator = mock(OrchestratorService.class);
        routing = new RoutingProperties();
        routing.setNewWorker(true);
        routing.setDataSourceWhitelist(List.of(WHITELISTED_DS));
        completedListener = new TaskCompletedListener(taskRepository, orchestrator, routing);
        failedListener = new TaskFailedListener(taskService, orchestrator, routing);
    }

    private EtlTask task(long dataSourceId) {
        EtlTask task = new EtlTask();
        task.setTaskId("task-1");
        task.setDataSourceId(dataSourceId);
        task.setStatus("running");
        return task;
    }

    private EtlTaskRun runningRun() {
        EtlTaskRun run = EtlTaskRun.newRun("run-1", "task-1", 1);
        ReflectionTestUtils.setField(run, "status", "RUNNING");
        return run;
    }

    @Test
    @DisplayName("白名单外任务 completed：旧链路直写 status/计数，不走状态机")
    void legacyCompletedStaysLegacy() {
        EtlTask task = task(LEGACY_DS);
        when(taskRepository.findByTaskId("task-1")).thenReturn(task);

        completedListener.listenTaskCompleted(
                "{\"taskId\":\"task-1\",\"completedRecords\":50,\"totalRecords\":50}");

        assertThat(task.getStatus()).isEqualTo("completed");
        assertThat(task.getCompletedRecords()).isEqualTo(50L);
        assertThat(task.getTotalRecords()).isEqualTo(50L);
        verify(taskRepository).save(task);
        verifyNoInteractions(orchestrator);
    }

    @Test
    @DisplayName("白名单任务 completed：走状态机 RUNNING→SUCCEEDED，不直写 task")
    void whitelistedCompletedRoutesToStateMachine() {
        when(taskRepository.findByTaskId("task-1")).thenReturn(task(WHITELISTED_DS));
        when(orchestrator.currentRun("task-1")).thenReturn(Optional.of(runningRun()));

        completedListener.listenTaskCompleted(
                "{\"taskId\":\"task-1\",\"completedRecords\":50,\"totalRecords\":50}");

        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.RUNNING),
                eq(EtlTaskStatus.SUCCEEDED), any());
        verify(taskRepository, never()).save(any());
    }

    @Test
    @DisplayName("白名单外任务 failed：旧链路 markFailed，不走状态机")
    void legacyFailedStaysLegacy() {
        when(taskService.getTaskByTaskId("task-1")).thenReturn(task(LEGACY_DS));

        failedListener.listen("{\"taskId\":\"task-1\",\"reason\":\"boom\"}");

        verify(taskService).markFailed("task-1", "boom");
        verifyNoInteractions(orchestrator);
    }

    @Test
    @DisplayName("白名单任务 failed：走状态机 RUNNING→NEEDS_ATTENTION，不调用 markFailed")
    void whitelistedFailedRoutesToStateMachine() {
        when(taskService.getTaskByTaskId("task-1")).thenReturn(task(WHITELISTED_DS));
        when(orchestrator.currentRun("task-1")).thenReturn(Optional.of(runningRun()));

        failedListener.listen("{\"taskId\":\"task-1\",\"reason\":\"boom\"}");

        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.RUNNING),
                eq(EtlTaskStatus.NEEDS_ATTENTION), any());
        verify(taskService, never()).markFailed(any(), any());
    }
}
