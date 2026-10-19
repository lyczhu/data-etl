package com.lawyus.dataetl.worker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.model.Shard;

/**
 * 分片派工消息（{@code etl.task.dispatch.batch}，方案 v2 WP-06/D10）：
 * 只带 {@code dataSourceId/targetDataSourceId} 与分片边界，<b>不再携带任何明文连接/口令</b>；
 * worker 运行时向 data-source-service 解析凭证。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class ShardDispatch {

    private String taskId;
    private String runId;
    private int attemptNo = 1;
    private String shardId;
    private Long pkFrom;
    private Long pkTo;
    private String resumeCursor;
    private String connectorType = ConnectorType.POSTGRES.name();

    private Long dataSourceId;
    private Long targetDataSourceId;
    private String sourceTable;
    private String targetTable;

    public ConnectorType connectorTypeEnum() {
        return ConnectorType.valueOf(connectorType);
    }

    public Shard toShard(String effectiveResumeCursor) {
        return new Shard(shardId, pkFrom, pkTo, null, null, effectiveResumeCursor);
    }

    // getters/setters（Jackson 反序列化需要）
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
