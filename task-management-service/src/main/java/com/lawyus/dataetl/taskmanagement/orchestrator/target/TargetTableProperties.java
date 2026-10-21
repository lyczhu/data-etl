package com.lawyus.dataetl.taskmanagement.orchestrator.target;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** 目标表治理配置（Phase 2E）。 */
@Component
@ConfigurationProperties(prefix = "etl.target-table")
public class TargetTableProperties {

    /** 终态 task 的目标表超过该天数才允许标记孤儿。 */
    private int orphanRetentionDays = 7;

    /** 是否启用每日孤儿标记（只标记，不删表）。 */
    private boolean cleanupEnabled = true;

    private String orphanCron = "0 0 4 * * *";

    public int getOrphanRetentionDays() {
        return orphanRetentionDays;
    }

    public void setOrphanRetentionDays(int orphanRetentionDays) {
        this.orphanRetentionDays = orphanRetentionDays;
    }

    public boolean isCleanupEnabled() {
        return cleanupEnabled;
    }

    public void setCleanupEnabled(boolean cleanupEnabled) {
        this.cleanupEnabled = cleanupEnabled;
    }

    public String getOrphanCron() {
        return orphanCron;
    }

    public void setOrphanCron(String orphanCron) {
        this.orphanCron = orphanCron;
    }
}
