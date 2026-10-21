package com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation;

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
 * {@code etl_reconciliation_run}（Phase 2D）：每个终态 run 的每日对账结果。
 */
@Entity
@Table(name = "etl_reconciliation_run")
@Access(AccessType.FIELD)
public class EtlReconciliationRun {

    public static final String PASS = "PASS";
    public static final String MISMATCH = "MISMATCH";
    public static final String ERROR = "ERROR";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "run_id", nullable = false, length = 64)
    private String runId;

    @Column(name = "task_id", nullable = false, length = 64)
    private String taskId;

    @Column(name = "status", nullable = false, length = 16)
    private String status;

    @Column(name = "source_rows")
    private Long sourceRows;

    @Column(name = "filtered_rows")
    private Long filteredRows;

    @Column(name = "failed_rows")
    private Long failedRows;

    @Column(name = "deduped_rows")
    private Long dedupedRows;

    @Column(name = "rows_in")
    private Long rowsIn;

    @Column(name = "rows_out")
    private Long rowsOut;

    @Column(name = "target_rows")
    private Long targetRows;

    @Column(name = "distinct_idempotency")
    private Long distinctIdempotency;

    @Column(name = "expected_target_rows")
    private Long expectedTargetRows;

    @Column(name = "diff")
    private Long diff;

    @Column(name = "details", columnDefinition = "text")
    private String details;

    @Column(name = "created_at", nullable = false)
    private Date createdAt;

    public EtlReconciliationRun() {
        // JPA 需要
    }

    public static EtlReconciliationRun of(String runId, String taskId) {
        EtlReconciliationRun row = new EtlReconciliationRun();
        row.runId = runId;
        row.taskId = taskId;
        row.createdAt = new Date();
        return row;
    }

    public Long getId() {
        return id;
    }

    public String getRunId() {
        return runId;
    }

    public String getTaskId() {
        return taskId;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getSourceRows() {
        return sourceRows;
    }

    public void setSourceRows(Long sourceRows) {
        this.sourceRows = sourceRows;
    }

    public Long getFilteredRows() {
        return filteredRows;
    }

    public void setFilteredRows(Long filteredRows) {
        this.filteredRows = filteredRows;
    }

    public Long getFailedRows() {
        return failedRows;
    }

    public void setFailedRows(Long failedRows) {
        this.failedRows = failedRows;
    }

    public Long getDedupedRows() {
        return dedupedRows;
    }

    public void setDedupedRows(Long dedupedRows) {
        this.dedupedRows = dedupedRows;
    }

    public Long getRowsIn() {
        return rowsIn;
    }

    public void setRowsIn(Long rowsIn) {
        this.rowsIn = rowsIn;
    }

    public Long getRowsOut() {
        return rowsOut;
    }

    public void setRowsOut(Long rowsOut) {
        this.rowsOut = rowsOut;
    }

    public Long getTargetRows() {
        return targetRows;
    }

    public void setTargetRows(Long targetRows) {
        this.targetRows = targetRows;
    }

    public Long getDistinctIdempotency() {
        return distinctIdempotency;
    }

    public void setDistinctIdempotency(Long distinctIdempotency) {
        this.distinctIdempotency = distinctIdempotency;
    }

    public Long getExpectedTargetRows() {
        return expectedTargetRows;
    }

    public void setExpectedTargetRows(Long expectedTargetRows) {
        this.expectedTargetRows = expectedTargetRows;
    }

    public Long getDiff() {
        return diff;
    }

    public void setDiff(Long diff) {
        this.diff = diff;
    }

    public String getDetails() {
        return details;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }
}
