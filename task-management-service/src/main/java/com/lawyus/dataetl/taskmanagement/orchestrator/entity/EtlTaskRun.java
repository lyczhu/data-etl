package com.lawyus.dataetl.taskmanagement.orchestrator.entity;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;

import javax.persistence.Access;
import javax.persistence.AccessType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;
import java.util.Date;

/**
 * {@code etl_task_run} 的 JPA 映射：ETL 执行实例（一次执行意图），run 层状态机的宿主。
 *
 * <p>设计约束（{@code docs/architecture/orchestrator-design.md §6.1 #3}）：
 * <b>不提供 public 的 {@code setStatus(EtlTaskStatus)} 写入口</b>。状态字段仅由
 * {@link com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository#casStatus}
 * 的乐观并发（CAS）bulk update 变更，唯一的业务写入路径是
 * {@link com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService#transition}。
 * 本类的状态字段在创建时（{@link #newRun}）写入一次 {@code CREATED}，之后只读。
 */
@Entity
@Table(name = "etl_task_run")
@Access(AccessType.FIELD)
public class EtlTaskRun {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "run_id", nullable = false, unique = true, length = 64)
    private String runId;

    @Column(name = "task_id", nullable = false, length = 64)
    private String taskId;

    @Column(name = "attempt_no", nullable = false)
    private Integer attemptNo;

    /** EtlTaskStatus 枚举名。仅 newRun 时写一次，后续经 CAS 变更，不暴露 setter。 */
    @Column(name = "status", nullable = false, length = 20)
    private String status;

    @Column(name = "is_current", nullable = false)
    private Boolean isCurrent;

    @Column(name = "queued_reason", length = 20)
    private String queuedReason;

    @Column(name = "shard_total", nullable = false)
    private Integer shardTotal;

    @Column(name = "shard_succeeded", nullable = false)
    private Integer shardSucceeded;

    @Column(name = "shard_failed", nullable = false)
    private Integer shardFailed;

    @Column(name = "shard_skipped", nullable = false)
    private Integer shardSkipped;

    @Column(name = "rows_in", nullable = false)
    private Long rowsIn;

    @Column(name = "rows_out", nullable = false)
    private Long rowsOut;

    @Column(name = "rows_dropped", nullable = false)
    private Long rowsDropped;

    @Column(name = "rows_deduped", nullable = false)
    private Long rowsDeduped;

    @Column(name = "next_attempt_at")
    private Date nextAttemptAt;

    @Column(name = "dispatched_at")
    private Date dispatchedAt;

    @Column(name = "last_progress_ts")
    private Date lastProgressTs;

    @Column(name = "cancel_requested_at")
    private Date cancelRequestedAt;

    @Column(name = "started_at")
    private Date startedAt;

    @Column(name = "finished_at")
    private Date finishedAt;

    @Column(name = "error_summary", columnDefinition = "text")
    private String errorSummary;

    @Column(name = "created_at")
    private Date createdAt;

    @Column(name = "updated_at")
    private Date updatedAt;

    public EtlTaskRun() {
        // JPA 需要
    }

    /**
     * 新建 run（T1）：状态固定为 {@link EtlTaskStatus#CREATED}，attempt 归 1，is_current=TRUE。
     * 这是本类唯一设置 status 的地方，且只在创建路径调用，不构成状态迁移入口。
     */
    public static EtlTaskRun newRun(String runId, String taskId, int attemptNo) {
        EtlTaskRun run = new EtlTaskRun();
        run.runId = runId;
        run.taskId = taskId;
        run.attemptNo = attemptNo;
        run.status = EtlTaskStatus.CREATED.name();
        run.isCurrent = Boolean.TRUE;
        run.shardTotal = 0;
        run.shardSucceeded = 0;
        run.shardFailed = 0;
        run.shardSkipped = 0;
        run.rowsIn = 0L;
        run.rowsOut = 0L;
        run.rowsDropped = 0L;
        run.rowsDeduped = 0L;
        return run;
    }

    @PrePersist
    protected void onCreate() {
        Date now = new Date();
        if (createdAt == null) {
            createdAt = now;
        }
        updatedAt = now;
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = new Date();
    }

    /** 状态枚举视图（读侧使用；写侧仅经 CAS）。 */
    public EtlTaskStatus statusEnum() {
        return EtlTaskStatus.valueOf(status);
    }

    // --- 只读 getter（无 status setter）---
    public Long getId() {
        return id;
    }

    public String getRunId() {
        return runId;
    }

    public String getTaskId() {
        return taskId;
    }

    public Integer getAttemptNo() {
        return attemptNo;
    }

    public String getStatus() {
        return status;
    }

    public Boolean getIsCurrent() {
        return isCurrent;
    }

    public String getQueuedReason() {
        return queuedReason;
    }

    public Integer getShardTotal() {
        return shardTotal;
    }

    public Integer getShardSucceeded() {
        return shardSucceeded;
    }

    public Integer getShardFailed() {
        return shardFailed;
    }

    public Integer getShardSkipped() {
        return shardSkipped;
    }

    public Long getRowsIn() {
        return rowsIn;
    }

    public Long getRowsOut() {
        return rowsOut;
    }

    public Long getRowsDropped() {
        return rowsDropped;
    }

    public Long getRowsDeduped() {
        return rowsDeduped;
    }

    public Date getNextAttemptAt() {
        return nextAttemptAt;
    }

    public Date getDispatchedAt() {
        return dispatchedAt;
    }

    public Date getLastProgressTs() {
        return lastProgressTs;
    }

    public Date getCancelRequestedAt() {
        return cancelRequestedAt;
    }

    public Date getStartedAt() {
        return startedAt;
    }

    public Date getFinishedAt() {
        return finishedAt;
    }

    public String getErrorSummary() {
        return errorSummary;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }
}
