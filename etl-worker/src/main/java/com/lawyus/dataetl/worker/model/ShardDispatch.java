package com.lawyus.dataetl.worker.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.model.Shard;
import com.lawyus.dataetl.connector.spi.model.SourceSpec;

/**
 * 分片派工消息（{@code etl.task.dispatch.batch}，方案 §5.2：只带游标区间不带数据行）。
 *
 * <p>骨架期为让 worker 可独立运行，源连接信息随消息携带（{@code source*} 字段）；
 * 正式实现应由 worker 用 {@code dataSourceId} 经 data-source-service（Feign + 缓存）解析连接，避免凭证走消息面。
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

    private String sourceUrl;
    private String sourceUsername;
    private String sourcePassword;
    private String sourceDriver;
    private String sourceTable;
    private String targetTable;

    public ConnectorType connectorTypeEnum() {
        return ConnectorType.valueOf(connectorType);
    }

    /** 源定位信息（JDBC 用 url/凭证；DriverManager 即时建连）。 */
    public SourceSpec sourceSpec() {
        return new SourceSpec(connectorTypeEnum(), sourceUrl, sourceUsername, sourcePassword, sourceDriver, sourceTable);
    }

    /** 目标定位（写 worker 自身 DataSource，此处仅带表名；{@code url} 供 ensureTargetTable 记录用）。 */
    public SourceSpec targetSpec() {
        return new SourceSpec(connectorTypeEnum(), sourceUrl, sourceUsername, sourcePassword, sourceDriver, targetTable);
    }

    public Shard toShard() {
        return new Shard(shardId, pkFrom, pkTo, null, null, resumeCursor);
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
    public String getSourceUrl() { return sourceUrl; }
    public void setSourceUrl(String sourceUrl) { this.sourceUrl = sourceUrl; }
    public String getSourceUsername() { return sourceUsername; }
    public void setSourceUsername(String sourceUsername) { this.sourceUsername = sourceUsername; }
    public String getSourcePassword() { return sourcePassword; }
    public void setSourcePassword(String sourcePassword) { this.sourcePassword = sourcePassword; }
    public String getSourceDriver() { return sourceDriver; }
    public void setSourceDriver(String sourceDriver) { this.sourceDriver = sourceDriver; }
    public String getSourceTable() { return sourceTable; }
    public void setSourceTable(String sourceTable) { this.sourceTable = sourceTable; }
    public String getTargetTable() { return targetTable; }
    public void setTargetTable(String targetTable) { this.targetTable = targetTable; }
}
