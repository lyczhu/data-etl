package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * 编排器侧的分片派工载荷（{@code etl.task.dispatch.batch}），字段名<b>严格对齐</b> worker 的
 * {@code worker.model.ShardDispatch}。
 *
 * <p>WP-06/D10：<b>不再携带任何明文连接信息/口令</b>，只带 {@code dataSourceId/targetDataSourceId}；
 * worker 运行时向 data-source-service 解析凭证。这样可以安全通过 Kafka topic/DLT/日志。
 */
@JsonInclude(JsonInclude.Include.NON_NULL)
public class ShardDispatchCommand {

    private String taskId;
    private String runId;
    private int attemptNo = 1;
    private String shardId;
    private Long pkFrom;
    private Long pkTo;
    private String resumeCursor;
    private String connectorType = "POSTGRES";

    private Long dataSourceId;
    private Long targetDataSourceId;
    private String sourceTable;
    private String targetTable;

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public int getAttemptNo() { return attemptNo; }
    public void setAttemptNo(int attemptNo) { this.attemptNo = attemptNo; }
    public String getShardId() { return shardId; }
    public void setShardId(String shardId) { this.shardId = shardId; }
    public Long getPkFrom() { return pkFrom; }
    public void setPkFrom(Long pkFrom) { this.pkFrom = pkFrom; }
    public Long getPkTo() { return pkTo; }
    public void setPkTo(Long pkTo) { this.pkTo = pkTo; }
    public String getResumeCursor() { return resumeCursor; }
    public void setResumeCursor(String resumeCursor) { this.resumeCursor = resumeCursor; }
    public String getConnectorType() { return connectorType; }
    public void setConnectorType(String connectorType) { this.connectorType = connectorType; }
    public Long getDataSourceId() { return dataSourceId; }
    public void setDataSourceId(Long dataSourceId) { this.dataSourceId = dataSourceId; }
    public Long getTargetDataSourceId() { return targetDataSourceId; }
    public void setTargetDataSourceId(Long targetDataSourceId) { this.targetDataSourceId = targetDataSourceId; }
    public String getSourceTable() { return sourceTable; }
    public void setSourceTable(String sourceTable) { this.sourceTable = sourceTable; }
    public String getTargetTable() { return targetTable; }
    public void setTargetTable(String targetTable) { this.targetTable = targetTable; }
}
