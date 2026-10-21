package com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/** 夜间对账配置（Phase 2D）。 */
@Component
@ConfigurationProperties(prefix = "etl.reconciliation")
public class ReconciliationProperties {

    private boolean enabled = true;

    /** 每日 02:30。 */
    private String cron = "0 30 2 * * *";

    /** 回看窗口：只对账最近 N 小时内结束的 run。 */
    private int lookbackHours = 48;

    /** 单轮最多处理多少个 run。 */
    private int batchSize = 100;

    /** 外部库 COUNT 查询超时（秒）。 */
    private int queryTimeoutSeconds = 30;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getCron() {
        return cron;
    }

    public void setCron(String cron) {
        this.cron = cron;
    }

    public int getLookbackHours() {
        return lookbackHours;
    }

    public void setLookbackHours(int lookbackHours) {
        this.lookbackHours = lookbackHours;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getQueryTimeoutSeconds() {
        return queryTimeoutSeconds;
    }

    public void setQueryTimeoutSeconds(int queryTimeoutSeconds) {
        this.queryTimeoutSeconds = queryTimeoutSeconds;
    }
}
