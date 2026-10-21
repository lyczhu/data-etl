package com.lawyus.dataetl.taskmanagement.orchestrator.target;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;
import java.util.Date;

/** {@code etl_target_table}（Phase 2E）：目标表归属与生命周期。 */
@Entity
@Table(name = "etl_target_table")
public class EtlTargetTable {

    public static final String ACTIVE = "ACTIVE";
    public static final String ORPHANED = "ORPHANED";
    public static final String PURGED = "PURGED";

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "task_id", nullable = false, length = 64)
    private String taskId;

    @Column(name = "target_data_source_id")
    private Long targetDataSourceId;

    @Column(name = "table_name", nullable = false, length = 100)
    private String tableName;

    @Column(name = "state", nullable = false, length = 16)
    private String state;

    @Column(name = "created_at", nullable = false)
    private Date createdAt;

    @Column(name = "last_used_at")
    private Date lastUsedAt;

    @Column(name = "purged_at")
    private Date purgedAt;

    public EtlTargetTable() {
        // JPA 需要
    }

    public static EtlTargetTable active(EtlTask task) {
        EtlTargetTable row = new EtlTargetTable();
        row.taskId = task.getTaskId();
        row.targetDataSourceId = task.getTargetDataSourceId();
        row.tableName = task.getTargetTableName();
        row.state = ACTIVE;
        row.createdAt = new Date();
        row.lastUsedAt = row.createdAt;
        return row;
    }

    public Long getId() {
        return id;
    }

    public String getTaskId() {
        return taskId;
    }

    public Long getTargetDataSourceId() {
        return targetDataSourceId;
    }

    public String getTableName() {
        return tableName;
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public Date getLastUsedAt() {
        return lastUsedAt;
    }

    public void setLastUsedAt(Date lastUsedAt) {
        this.lastUsedAt = lastUsedAt;
    }

    public Date getPurgedAt() {
        return purgedAt;
    }

    public void setPurgedAt(Date purgedAt) {
        this.purgedAt = purgedAt;
    }
}
