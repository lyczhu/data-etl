package com.lawyus.dataetl.worker.model;

/**
 * 分片终态回报（{@code etl.task.results}，方案 §7.5）。results 只是<b>触发器</b>，终局由 orchestrator 重算聚合判定。
 */
public class ShardResultMessage {

    private String taskId;
    private String runId;
    private String shardId;
    /** SUCCEEDED / FAILED（对应 {@link com.lawyus.dataetl.connector.spi.model.SinkResult} 的落点）。 */
    private String status;
    private long rowsIn;
    private long rowsOut;
    private long rowsDropped;
    private long rowsDeduped;
    private String errorClass;
    private String errorDetail;

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public String getShardId() { return shardId; }
    public void setShardId(String shardId) { this.shardId = shardId; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public long getRowsIn() { return rowsIn; }
    public void setRowsIn(long rowsIn) { this.rowsIn = rowsIn; }
    public long getRowsOut() { return rowsOut; }
    public void setRowsOut(long rowsOut) { this.rowsOut = rowsOut; }
    public long getRowsDropped() { return rowsDropped; }
    public void setRowsDropped(long rowsDropped) { this.rowsDropped = rowsDropped; }
    public long getRowsDeduped() { return rowsDeduped; }
    public void setRowsDeduped(long rowsDeduped) { this.rowsDeduped = rowsDeduped; }
    public String getErrorClass() { return errorClass; }
    public void setErrorClass(String errorClass) { this.errorClass = errorClass; }
    public String getErrorDetail() { return errorDetail; }
    public void setErrorDetail(String errorDetail) { this.errorDetail = errorDetail; }
}
