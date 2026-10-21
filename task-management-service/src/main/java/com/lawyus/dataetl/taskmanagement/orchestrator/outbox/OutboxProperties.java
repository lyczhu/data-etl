package com.lawyus.dataetl.taskmanagement.orchestrator.outbox;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 派工 Outbox 配置（Phase 2C）。默认关闭，灰度通过 config-server 打开。
 */
@Component
@ConfigurationProperties(prefix = "etl.outbox")
public class OutboxProperties {

    /** true：派工先写 outbox 再由 poller 投递；false：保持直投 Kafka（回滚开关）。 */
    private boolean enabled = false;

    /** poller 轮询间隔（毫秒）。 */
    private long pollIntervalMs = 1000;

    /** 单轮最多投递行数。 */
    private int batchSize = 200;

    /** 投递失败达到该次数后转人工（仍会按最大退避重试）。 */
    private int maxAttempts = 5;

    /** 失败退避秒数序列；超出长度后取最后一个值。 */
    private List<Long> backoffSeconds = new ArrayList<>(List.of(1L, 5L, 30L, 120L, 600L));

    /** 已发送行保留天数；<=0 表示不清理。 */
    private int retentionDays = 7;

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public long getPollIntervalMs() {
        return pollIntervalMs;
    }

    public void setPollIntervalMs(long pollIntervalMs) {
        this.pollIntervalMs = pollIntervalMs;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public List<Long> getBackoffSeconds() {
        return backoffSeconds;
    }

    public void setBackoffSeconds(List<Long> backoffSeconds) {
        this.backoffSeconds = backoffSeconds;
    }

    public int getRetentionDays() {
        return retentionDays;
    }

    public void setRetentionDays(int retentionDays) {
        this.retentionDays = retentionDays;
    }

    /** 第 attempt 次失败后的退避秒数（attempt 从 1 开始）。 */
    public long backoffFor(int attempt) {
        if (backoffSeconds == null || backoffSeconds.isEmpty()) {
            return 1L;
        }
        int index = Math.min(Math.max(attempt, 1) - 1, backoffSeconds.size() - 1);
        return Math.max(1L, backoffSeconds.get(index));
    }
}
