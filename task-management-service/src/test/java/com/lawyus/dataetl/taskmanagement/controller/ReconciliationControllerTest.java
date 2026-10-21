package com.lawyus.dataetl.taskmanagement.controller;

import com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation.EtlReconciliationRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation.ReconciliationService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** Phase 2D：对账查询/重算 API 的 200/404 契约。 */
class ReconciliationControllerTest {

    private ReconciliationService service;
    private MockMvc mvc;

    @BeforeEach
    void setUp() {
        service = mock(ReconciliationService.class);
        ReconciliationController controller = new ReconciliationController();
        ReflectionTestUtils.setField(controller, "reconciliationService", service);
        mvc = MockMvcBuilders.standaloneSetup(controller).build();
    }

    @Test
    @DisplayName("GET 已存在：200；不存在：404")
    void getMapsFoundAndNotFound() throws Exception {
        EtlReconciliationRun row = EtlReconciliationRun.of("r1", "t1");
        row.setStatus(EtlReconciliationRun.PASS);
        when(service.find("r1")).thenReturn(Optional.of(row));
        when(service.find("missing")).thenReturn(Optional.empty());

        mvc.perform(get("/api/reconciliation/r1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PASS"));
        mvc.perform(get("/api/reconciliation/missing"))
                .andExpect(status().isNotFound());
    }

    @Test
    @DisplayName("POST 重算：200；run 不存在：404")
    void postMapsFoundAndNotFound() throws Exception {
        EtlReconciliationRun row = EtlReconciliationRun.of("r1", "t1");
        row.setStatus(EtlReconciliationRun.MISMATCH);
        when(service.reconcile("r1", true)).thenReturn(row);
        when(service.reconcile("missing", true))
                .thenThrow(new IllegalArgumentException("run not found: missing"));

        mvc.perform(post("/api/reconciliation/r1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("MISMATCH"));
        mvc.perform(post("/api/reconciliation/missing"))
                .andExpect(status().isNotFound());
    }
}
