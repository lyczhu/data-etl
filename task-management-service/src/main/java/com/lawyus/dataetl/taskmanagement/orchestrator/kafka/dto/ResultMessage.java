package com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 分片终态消息（{@code etl.task.results}），字段对齐 worker {@code ShardResultMessage}。
 * results 只是收敛<b>触发器</b>，其自报统计不参与终局判定（P1/D-16）。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ResultMessage {

    private String taskId;
    private String runId;
    private String shardId;
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
