package com.lawyus.dataetl.taskmanagement.orchestrator.audit;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 审计保留策略（Phase 2B）。默认 180 天，与 orchestrator-design Q3 的“宽恕审计保留 180d”一致。
 */
@Component
@ConfigurationProperties(prefix = "etl.audit")
public class AuditProperties {

    /** 审计保留天数；<=0 表示不清理（安全开关）。 */
    private int retentionDays = 180;

    /** 清理任务 cron（默认每日 03:00）。 */
    private String retentionCron = "0 0 3 * * *";

    public int getRetentionDays() {
        return retentionDays;
    }

    public void setRetentionDays(int retentionDays) {
        this.retentionDays = retentionDays;
    }

    public String getRetentionCron() {
        return retentionCron;
    }

    public void setRetentionCron(String retentionCron) {
        this.retentionCron = retentionCron;
    }
}
