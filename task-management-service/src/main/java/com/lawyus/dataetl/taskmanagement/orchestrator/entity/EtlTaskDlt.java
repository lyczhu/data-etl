package com.lawyus.dataetl.taskmanagement.orchestrator.entity;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

/**
 * {@code etl_task_dlt} 的 JPA 映射（WP-04/WP-05）：worker 直写，orchestrator 只读
 * “未解决 DLT 数”用于 G-7 与 {@code /failures} 查询。
 */
@Entity
@Table(name = "etl_task_dlt")
public class EtlTaskDlt {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "task_id", nullable = false, length = 64)
    private String taskId;

    @Column(name = "run_id", nullable = false, length = 64)
    private String runId;

    @Column(name = "shard_id", length = 32)
    private String shardId;

    @Column(name = "error_class", nullable = false, length = 24)
    private String errorClass;

    @Column(name = "error_message", columnDefinition = "text")
    private String errorMessage;

    @Column(name = "raw_payload", columnDefinition = "text")
    private String rawPayload;

    @Column(name = "resolved_at")
    private Date resolvedAt;

    @Column(name = "abandoned", nullable = false)
    private boolean abandoned;

    @Column(name = "created_at", nullable = false)
    private Date createdAt;

    public Long getId() { return id; }
    public String getTaskId() { return taskId; }
    public String getRunId() { return runId; }
    public String getShardId() { return shardId; }
    public String getErrorClass() { return errorClass; }
    public String getErrorMessage() { return errorMessage; }
    public String getRawPayload() { return rawPayload; }
    public Date getResolvedAt() { return resolvedAt; }
    public boolean isAbandoned() { return abandoned; }
    public Date getCreatedAt() { return createdAt; }
}
