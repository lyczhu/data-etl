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

    /** JDBC 分片主键闭区间下界，validate/T2 落库时由编排器写入；worker 只读（供 pump 派工，§7.2）。 */
    @Column(name = "pk_from")
    private Long pkFrom;

    /** JDBC 分片主键闭区间上界（非 JDBC 源可为空）。 */
    @Column(name = "pk_to")
    private Long pkTo;

    @Column(name = "rows_in", nullable = false)
    private Long rowsIn;

    @Column(name = "rows_out", nullable = false)
    private Long rowsOut;

    @Column(name = "rows_dropped", nullable = false)
    private Long rowsDropped;

    /** 业务规则过滤（transformer 返回 null），可进成功等式（WP-04）。 */
    @Column(name = "rows_filtered", nullable = false)
    private Long rowsFiltered;

    /** 写入/解析失败（已落 DLT），>0 时禁止 SUCCEEDED（WP-04）。 */
    @Column(name = "rows_failed", nullable = false)
    private Long rowsFailed;

    @Column(name = "rows_deduped", nullable = false)
    private Long rowsDeduped;

    @Column(name = "lease_owner", length = 128)
    private String leaseOwner;

    @Column(name = "lease_until")
    private Date leaseUntil;

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
        return newPending(taskId, runId, shardId, null, null);
    }

    /**
     * T2 校验通过后批量生成的 PENDING 分片行，携带 JDBC 主键闭区间
     * （{@code [pkFrom, pkTo]}，供 {@code DispatchService.pump} 派工时回填，§7.2）。
     */
    public static EtlTaskCheckpoint newPending(String taskId, String runId, String shardId, Long pkFrom, Long pkTo) {
        EtlTaskCheckpoint c = new EtlTaskCheckpoint();
        c.taskId = taskId;
        c.runId = runId;
        c.shardId = shardId;
        c.pkFrom = pkFrom;
        c.pkTo = pkTo;
        c.status = EtlShardStatus.PENDING.name();
        c.shardAttempt = 0;
        c.rowsIn = 0L;
        c.rowsOut = 0L;
        c.rowsDropped = 0L;
        c.rowsFiltered = 0L;
        c.rowsFailed = 0L;
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

    public Long getPkFrom() {
        return pkFrom;
    }

    public Long getPkTo() {
        return pkTo;
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

    public Long getRowsFiltered() {
        return rowsFiltered;
    }

    public Long getRowsFailed() {
        return rowsFailed;
    }

    public Long getRowsDeduped() {
        return rowsDeduped;
    }

    public String getLeaseOwner() {
        return leaseOwner;
    }

    public Date getLeaseUntil() {
        return leaseUntil;
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
