package com.lawyus.dataetl.taskmanagement.orchestrator.entity;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlShardStatus;

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
 * {@code etl_task_checkpoint} 的 JPA 映射：分片断点，断点续传唯一真源。
 *
 * <p>写权矩阵（{@code orchestrator-design.md §4.1}）：本表由 <b>worker</b> 直写
 * （与目标表写入同一本地事务）；orchestrator 侧仅只读聚合，
 * 只有 {@code /retry?scope=failed-shards}、{@code /abort}、{@code /mark-succeeded-with-data-loss}
 * 可改 {@code status}。因此本类只提供读侧 getter 与 T2 批量建 PENDING 的工厂方法。
 */
@Entity
@Table(name = "etl_task_checkpoint")
@Access(AccessType.FIELD)
public class EtlTaskCheckpoint {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "task_id", nullable = false, length = 64)
    private String taskId;

    @Column(name = "run_id", nullable = false, length = 64)
    private String runId;

    @Column(name = "shard_id", nullable = false, length = 32)
    private String shardId;

    @Column(name = "status", nullable = false, length = 16)
    private String status;

    @Column(name = "shard_attempt", nullable = false)
    private Integer shardAttempt;

    @Column(name = "last_cursor", columnDefinition = "text")
    private String lastCursor;

    @Column(name = "last_pk", length = 128)
    private String lastPk;

    @Column(name = "rows_in", nullable = false)
    private Long rowsIn;

    @Column(name = "rows_out", nullable = false)
    private Long rowsOut;

    @Column(name = "rows_dropped", nullable = false)
    private Long rowsDropped;

    @Column(name = "rows_deduped", nullable = false)
    private Long rowsDeduped;

    @Column(name = "error_class", length = 24)
    private String errorClass;

    @Column(name = "error_detail", columnDefinition = "text")
    private String errorDetail;

    @Column(name = "updated_at", nullable = false)
    private Date updatedAt;

    public EtlTaskCheckpoint() {
        // JPA 需要
    }

    /** T2 校验通过后批量生成的分片计划行（{@link EtlShardStatus#PENDING}）。 */
    public static EtlTaskCheckpoint newPending(String taskId, String runId, String shardId) {
        EtlTaskCheckpoint c = new EtlTaskCheckpoint();
        c.taskId = taskId;
        c.runId = runId;
        c.shardId = shardId;
        c.status = EtlShardStatus.PENDING.name();
        c.shardAttempt = 0;
        c.rowsIn = 0L;
        c.rowsOut = 0L;
        c.rowsDropped = 0L;
        c.rowsDeduped = 0L;
        c.updatedAt = new Date();
        return c;
    }

    @PrePersist
    protected void onPrePersist() {
        if (updatedAt == null) {
            updatedAt = new Date();
        }
    }

    @PreUpdate
    protected void onPreUpdate() {
        updatedAt = new Date();
    }

    public EtlShardStatus statusEnum() {
        return EtlShardStatus.valueOf(status);
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

    public String getShardId() {
        return shardId;
    }

    public String getStatus() {
        return status;
    }

    public Integer getShardAttempt() {
        return shardAttempt;
    }

    public String getLastCursor() {
        return lastCursor;
    }

    public String getLastPk() {
        return lastPk;
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

    public String getErrorClass() {
        return errorClass;
    }

    public String getErrorDetail() {
        return errorDetail;
    }

    public Date getUpdatedAt() {
        return updatedAt;
    }
}
