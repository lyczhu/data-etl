package com.lawyus.dataetl.taskmanagement.controller;

import com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation.EtlReconciliationRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation.ReconciliationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 对账结果查询与人工重算（Phase 2D）。
 *
 * <ul>
 *   <li>{@code GET /api/reconciliation/{runId}}：单 run 结果；</li>
 *   <li>{@code GET /api/reconciliation?taskId=...}：按 task 分页；</li>
 *   <li>{@code POST /api/reconciliation/{runId}}：强制重算（覆盖历史结果）。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/reconciliation")
public class ReconciliationController {

    @Autowired
    private ReconciliationService reconciliationService;

    @GetMapping("/{runId}")
    public ResponseEntity<EtlReconciliationRun> get(@PathVariable String runId) {
        return reconciliationService.find(runId)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    @GetMapping
    public Page<EtlReconciliationRun> page(@RequestParam String taskId,
                                           @RequestParam(defaultValue = "0") int page,
                                           @RequestParam(defaultValue = "50") int size) {
        return reconciliationService.page(taskId, page, size);
    }

    @PostMapping("/{runId}")
    public ResponseEntity<?> reconcile(@PathVariable String runId) {
        try {
            return ResponseEntity.ok(reconciliationService.reconcile(runId, true));
        } catch (IllegalArgumentException e) {
            return ResponseEntity.notFound().build();
        }
    }
}
