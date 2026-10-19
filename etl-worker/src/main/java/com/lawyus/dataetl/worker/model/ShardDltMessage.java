package com.lawyus.dataetl.worker.model;

/**
 * 分片级 DLT 消息（{@code etl.task.dlt}）：worker 在坏行/分片失败时发送，
 * orchestrator 的 {@code DltListener} 据此把 run 迁到 {@code NEEDS_ATTENTION}。
 */
public class ShardDltMessage {

    private String taskId;
    private String runId;
    private String shardId;
    private String errorClass;
    private String errorMessage;
    private long failedRows;
    private long occurredAt;

    public ShardDltMessage() {
    }

    public ShardDltMessage(String taskId, String runId, String shardId,
                           String errorClass, String errorMessage, long failedRows) {
        this.taskId = taskId;
        this.runId = runId;
        this.shardId = shardId;
        this.errorClass = errorClass;
        this.errorMessage = errorMessage;
        this.failedRows = failedRows;
        this.occurredAt = System.currentTimeMillis();
    }

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public String getShardId() { return shardId; }
    public void setShardId(String shardId) { this.shardId = shardId; }
    public String getErrorClass() { return errorClass; }
    public void setErrorClass(String errorClass) { this.errorClass = errorClass; }
    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }
    public long getFailedRows() { return failedRows; }
    public void setFailedRows(long failedRows) { this.failedRows = failedRows; }
    public long getOccurredAt() { return occurredAt; }
    public void setOccurredAt(long occurredAt) { this.occurredAt = occurredAt; }
}
