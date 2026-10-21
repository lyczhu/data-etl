package com.lawyus.dataetl.taskmanagement.orchestrator.audit;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskAudit;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskAuditRepository;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

/**
 * 审计只读查询（Phase 2B）：{@code GET /api/tasks/{taskId}/audit}。
 *
 * <p>分页大小上限 200，避免一次拉爆；task 不存在时抛 IllegalArgumentException（控制器映射 404）。
 */
@Service
public class AuditService {

    private static final int MAX_PAGE_SIZE = 200;

    private final EtlTaskRepository taskRepository;
    private final EtlTaskAuditRepository auditRepository;

    public AuditService(EtlTaskRepository taskRepository, EtlTaskAuditRepository auditRepository) {
        this.taskRepository = taskRepository;
        this.auditRepository = auditRepository;
    }

    public Page<EtlTaskAudit> page(String taskId, int page, int size) {
        if (taskRepository.findByTaskId(taskId) == null) {
            throw new IllegalArgumentException("task not found: " + taskId);
        }
        int safePage = Math.max(0, page);
        int safeSize = Math.min(MAX_PAGE_SIZE, Math.max(1, size));
        return auditRepository.findByTaskIdOrderByIdDesc(taskId, PageRequest.of(safePage, safeSize));
    }
}
