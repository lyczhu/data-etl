package com.lawyus.dataetl.taskmanagement.controller;

import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorCommandService;
import com.lawyus.dataetl.taskmanagement.orchestrator.audit.AuditService;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskAudit;
import com.lawyus.dataetl.taskmanagement.orchestrator.target.TargetTableConflictException;
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Date;
import java.util.List;

import org.springframework.data.domain.PageImpl;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * WP-14 / Gate 2：人工干预 API 的最小闭环与错误语义（200/202/404/409）。
 */
class EtlTaskControllerTest {

    private EtlTaskController controller;
    private EtlTaskService taskService;
    private OrchestratorCommandService commandService;
    private AuditService auditService;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        controller = new EtlTaskController();
        taskService = mock(EtlTaskService.class);
        commandService = mock(OrchestratorCommandService.class);
        auditService = mock(AuditService.class);
        ReflectionTestUtils.setField(controller, "etlTaskService", taskService);
        ReflectionTestUtils.setField(controller, "commandService", commandService);
        ReflectionTestUtils.setField(controller, "auditService", auditService);
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    private static OrchestratorCommandService.ProgressView progress() {
        return new OrchestratorCommandService.ProgressView(
                "t1", "r1", "RUNNING", 1, 3,
                0, 1, 2, 0, 0, 0,
                20, 7, 0, 0, 0, false, new Date(), null);
    }

    @Test
    @DisplayName("GET /progress 返回 run 状态与分片/行计数")
    void progressReturnsView() throws Exception {
        when(commandService.progress("t1")).thenReturn(progress());

        mvc.perform(get("/api/tasks/t1/progress"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("RUNNING"))
                .andExpect(jsonPath("$.shardTotal").value(3))
                .andExpect(jsonPath("$.running").value(2));
    }

    @Test
    @DisplayName("GET /audit 返回审计分页；task 不存在 → 404")
    void auditReturnsPageAndMapsNotFound() throws Exception {
        EtlTaskAudit audit = EtlTaskAudit.record("t1", "r1", "CREATED", "VALIDATED",
                "it", "VALIDATED", null, null);
        when(auditService.page("t1", 0, 50)).thenReturn(new PageImpl<>(List.of(audit)));
        mvc.perform(get("/api/tasks/t1/audit"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.content[0].toStatus").value("VALIDATED"))
                .andExpect(jsonPath("$.totalElements").value(1));

        when(auditService.page("missing", 0, 50))
                .thenThrow(new IllegalArgumentException("task not found: missing"));
        mvc.perform(get("/api/tasks/missing/audit"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("目标表冲突 → 409；冻结字段修改 → 400")
    void targetTableConflictAndFrozenFieldsMapped() throws Exception {
        when(taskService.createTask(any())).thenThrow(new TargetTableConflictException("already owned"));
        mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isConflict());

        when(taskService.updateTask(org.mockito.ArgumentMatchers.eq(1L), any()))
                .thenThrow(new IllegalArgumentException("targetTableName is immutable after task creation"));
        mvc.perform(put("/api/tasks/1").contentType(MediaType.APPLICATION_JSON).content("{}"))
                .andExpect(status().isBadRequest());
    }

    @Test
    @DisplayName("POST /cancel 返回 202 且不等待 worker 收敛")
    void cancelAccepted() throws Exception {
        mvc.perform(post("/api/tasks/t1/cancel"))
                .andExpect(status().isAccepted())
                .andExpect(content().string("CANCEL_REQUESTED"));
    }

    @Test
    @DisplayName("无 current run → 404；终态/非法状态 → 409")
    void mapsNotFoundAndConflict() throws Exception {
        doThrow(new IllegalArgumentException("no current run for task t1"))
                .when(commandService).cancel("t1");
        mvc.perform(post("/api/tasks/t1/cancel"))
                .andExpect(status().isNotFound());

        doThrow(new IllegalStateException("run already terminal: SUCCEEDED"))
                .when(commandService).retry("t1");
        mvc.perform(post("/api/tasks/t1/retry"))
                .andExpect(status().isConflict());
    }
}
