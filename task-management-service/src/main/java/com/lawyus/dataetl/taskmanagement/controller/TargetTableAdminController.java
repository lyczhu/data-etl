package com.lawyus.dataetl.taskmanagement.controller;

import com.lawyus.dataetl.taskmanagement.orchestrator.target.EtlTargetTable;
import com.lawyus.dataetl.taskmanagement.orchestrator.target.TargetTableRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Page;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 * 目标表治理 API（Phase 2E）。
 *
 * <ul>
 *   <li>{@code GET /api/admin/target-tables?state=ACTIVE|ORPHANED|PURGED}：登记分页；</li>
 *   <li>{@code POST /api/admin/target-tables/cleanup?dryRun=true}：默认只列候选；</li>
 *   <li>{@code POST ...&dryRun=false}：删除 ORPHANED 目标表并标 PURGED。</li>
 * </ul>
 */
@RestController
@RequestMapping("/api/admin/target-tables")
public class TargetTableAdminController {

    @Autowired
    private TargetTableRegistry registry;

    @GetMapping
    public Page<EtlTargetTable> page(@RequestParam(required = false) String state,
                                     @RequestParam(defaultValue = "0") int page,
                                     @RequestParam(defaultValue = "50") int size) {
        return registry.page(state, page, size);
    }

    @PostMapping("/cleanup")
    public TargetTableRegistry.CleanupReport cleanup(
            @RequestParam(defaultValue = "true") boolean dryRun,
            @RequestParam(defaultValue = "7") int retentionDays) {
        return registry.cleanup(dryRun, retentionDays);
    }
}
