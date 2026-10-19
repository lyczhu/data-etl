package com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

/**
 * 毒消息/DLT 事件（{@code etl.task.dlt}）。字段对齐设计文档 §7.6 的 {@code etl_task_dlt}（Phase 2 落表）
 * 与非法迁移转储（§6.4）。Phase 1 编排器消费本消息只做控制动作（触发 {@code NEEDS_ATTENTION}），
 * 完整 DLT 落表/回写（{@code resolved_at}/{@code abandoned}）随 Phase 2 的 {@code 07-etl-task-dlt.sql}。
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class DltMessage {

    private String taskId;
    private String runId;
    private String shardId;
    /** FATAL | ILLEGAL_TRANSITION | PROTOCOL_VIOLATION 等。 */
    private String errorClass;
    private String errorDetail;
    private String rawPayload;
    private String kafkaTopic;
    private Integer kafkaPartition;
    private Long kafkaOffset;

    public String getTaskId() { return taskId; }
    public void setTaskId(String taskId) { this.taskId = taskId; }
    public String getRunId() { return runId; }
    public void setRunId(String runId) { this.runId = runId; }
    public String getShardId() { return shardId; }
    public void setShardId(String shardId) { this.shardId = shardId; }
    public String getErrorClass() { return errorClass; }
    public void setErrorClass(String errorClass) { this.errorClass = errorClass; }
    public String getErrorDetail() { return errorDetail; }
    public void setErrorDetail(String errorDetail) { this.errorDetail = errorDetail; }
    public String getRawPayload() { return rawPayload; }
    public void setRawPayload(String rawPayload) { this.rawPayload = rawPayload; }
    public String getKafkaTopic() { return kafkaTopic; }
    public void setKafkaTopic(String kafkaTopic) { this.kafkaTopic = kafkaTopic; }
    public Integer getKafkaPartition() { return kafkaPartition; }
    public void setKafkaPartition(Integer kafkaPartition) { this.kafkaPartition = kafkaPartition; }
    public Long getKafkaOffset() { return kafkaOffset; }
    public void setKafkaOffset(Long kafkaOffset) { this.kafkaOffset = kafkaOffset; }
}
