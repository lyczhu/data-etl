package com.lawyus.dataetl.taskmanagement.orchestrator.entity;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

/**
 * {@code etl_task_audit} 的 JPA 映射（Phase 2B / INV-9）：
 * 每一次真正落地的状态迁移恰好一条审计，与 {@code OrchestratorService.transition()} 同事务写入。
 *
 * <p>不建外键：审计必须比 task/run 活得更久，不能被级联删除。
 */
@Entity
@Table(name = "etl_task_audit")
@Access(AccessType.FIELD)
public class EtlTaskAudit {

    /** 新建 run 的“迁移前状态”哨兵值。 */
    public static final String NONE = "(none)";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "task_id", nullable = false, length = 64)
    private String taskId;

    @Column(name = "run_id", length = 64)
    private String runId;

    @Column(name = "from_status", nullable = false, length = 24)
    private String fromStatus;

    @Column(name = "to_status", nullable = false, length = 24)
    private String toStatus;

    @Column(name = "actor", nullable = false, length = 64)
    private String actor;

    @Column(name = "reason", length = 256)
    private String reason;

    @Column(name = "error_class", length = 24)
    private String errorClass;

    @Column(name = "context", columnDefinition = "text")
    private String context;

    @Column(name = "created_at", nullable = false)
    private Date createdAt;

    public EtlTaskAudit() {
        // JPA 需要
    }

    public static EtlTaskAudit record(String taskId, String runId, String fromStatus, String toStatus,
                                      String actor, String reason, String errorClass, String context) {
        EtlTaskAudit audit = new EtlTaskAudit();
        audit.taskId = taskId;
        audit.runId = runId;
        audit.fromStatus = fromStatus;
        audit.toStatus = toStatus;
        audit.actor = actor == null || actor.isBlank() ? "unknown" : truncate(actor, 64);
        audit.reason = truncate(reason, 256);
        audit.errorClass = truncate(errorClass, 24);
        audit.context = context;
        audit.createdAt = new Date();
        return audit;
    }

    private static String truncate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() <= max ? value : value.substring(0, max);
    }

    public Long getId() {
        return id;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getRunId() {
        return runId;
    }

    public String getFromStatus() {
        return fromStatus;
    }

    public String getToStatus() {
        return toStatus;
    }

    public String getActor() {
        return actor;
    }

    public String getReason() {
        return reason;
    }

    public String getErrorClass() {
        return errorClass;
    }

    public String getContext() {
        return context;
    }

    public Date getCreatedAt() {
        return createdAt;
    }
}
