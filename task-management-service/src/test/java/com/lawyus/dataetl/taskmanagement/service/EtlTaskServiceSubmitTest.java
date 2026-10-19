package com.lawyus.dataetl.taskmanagement.service;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.validate.RunValidator;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class EtlTaskServiceSubmitTest {

    private EtlTaskRepository etlTaskRepository;
    private OrchestratorService orchestrator;
    private RunValidator runValidator;
    private RoutingProperties routing;
    private EtlTaskService service;

    @BeforeEach
    void setUp() {
        etlTaskRepository = mock(EtlTaskRepository.class);
        orchestrator = mock(OrchestratorService.class);
        runValidator = mock(RunValidator.class);
        routing = new RoutingProperties();
        service = new EtlTaskService();
        ReflectionTestUtils.setField(service, "etlTaskRepository", etlTaskRepository);
        ReflectionTestUtils.setField(service, "orchestrator", orchestrator);
        ReflectionTestUtils.setField(service, "runValidator", runValidator);
        ReflectionTestUtils.setField(service, "routing", routing);
        when(etlTaskRepository.save(org.mockito.ArgumentMatchers.any(EtlTask.class)))
                .thenAnswer(inv -> inv.getArgument(0));
    }

    private EtlTask input() {
        EtlTask t = new EtlTask();
        t.setDataSourceId(1L);
        t.setSourceTableName("orders");
        return t;
    }

    @Test
    @DisplayName("newWorker=true：提交即建 run 并触发校验入队")
    void createsRunAndValidatesWhenNewWorkerOn() {
        routing.setNewWorker(true);
        EtlTaskRun run = EtlTaskRun.newRun("run-x", "any", 1);
        when(orchestrator.createRun(anyString())).thenReturn(run);

        EtlTask saved = service.createTask(input());

        verify(orchestrator).createRun(eq(saved.getTaskId()));
        verify(runValidator).validateAndQueue(eq(saved.getTaskId()), eq("run-x"));
    }

    @Test
    @DisplayName("newWorker=false（默认）：不建 run、不校验，旧链路行为不变")
    void dormantWhenNewWorkerOff() {
        routing.setNewWorker(false);

        service.createTask(input());

        verify(orchestrator, never()).createRun(anyString());
        verify(runValidator, never()).validateAndQueue(anyString(), anyString());
    }

    @Test
    @DisplayName("校验链基础设施异常被吞（不回滚 HTTP 提交），run 已建")
    void submitSwallowsValidateFailure() {
        routing.setNewWorker(true);
        EtlTaskRun run = EtlTaskRun.newRun("run-x", "any", 1);
        when(orchestrator.createRun(anyString())).thenReturn(run);
        org.mockito.Mockito.doThrow(new RuntimeException("feign down"))
                .when(runValidator).validateAndQueue(anyString(), anyString());

        EtlTask saved = service.createTask(input());   // 不应抛出

        org.assertj.core.api.Assertions.assertThat(saved.getTaskId()).isNotBlank();
    }
}
