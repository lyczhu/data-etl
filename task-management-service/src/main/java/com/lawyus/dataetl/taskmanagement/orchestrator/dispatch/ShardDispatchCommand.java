package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.fasterxml.jackson.annotation.JsonInclude;

/**
 * 编排器侧的分片派工载荷（{@code etl.task.dispatch.batch}），字段名<b>严格对齐</b> worker 的
 * {@code worker.model.ShardDispatch}，保证跨服务 JSON 兼容；编排器不依赖 {@code etl-worker} 模块，
 * 故在本模块独立声明（{@code orchestrator-design.md §7.2}：只带游标区间不带数据行）。
 *
 * <p>凭证随消息面是 Phase 1 骨架妥协（让 worker 可独立跑）；正式实现应由 worker 用 {@code dataSourceId}
 * 经 data-source-service 解析连接。此处保留 {@code dataSourceId} 字段为将来收敛做准备。
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
    private String sourceUrl;
    private String sourceUsername;
    private String sourcePassword;
    private String sourceDriver;
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
