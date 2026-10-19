package com.lawyus.dataetl.worker.model;

/**
 * 分片进度/心跳回报（{@code etl.task.progress}，每 shard ≤1/5s，方案 §7.3）。
 * progress 只服务心跳确认与卡死判定基线，<b>不推进业务状态</b>（除首次 T7）。
 */
public class ShardProgress {

    private String taskId;
    private String runId;
    private String shardId;
    private long rowsIn;
    private long rowsOut;
    private long timestamp = System.currentTimeMillis();

    public ShardProgress() {
    }

    public ShardProgress(String taskId, String runId, String shardId, long rowsIn, long rowsOut, long timestamp) {
        this.taskId = taskId;
        this.runId = runId;
        this.shardId = shardId;
        this.rowsIn = rowsIn;
        this.rowsOut = rowsOut;
        this.timestamp = timestamp;
    }

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
