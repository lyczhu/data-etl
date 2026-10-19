package com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 分片进度/心跳消息（{@code etl.task.progress}），字段对齐 worker {@code ShardProgress}。
 * 编排器侧独立声明，不依赖 {@code etl-worker}。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ProgressMessage {

    private String taskId;
    private String runId;
    private String shardId;
    private long rowsIn;
    private long rowsOut;
    private long timestamp;

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public String getShardId() { return shardId; }
    public void setShardId(String shardId) { this.shardId = shardId; }
    public long getRowsIn() { return rowsIn; }
    public void setRowsIn(long rowsIn) { this.rowsIn = rowsIn; }
    public long getRowsOut() { return rowsOut; }
    public void setRowsOut(long rowsOut) { this.rowsOut = rowsOut; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
