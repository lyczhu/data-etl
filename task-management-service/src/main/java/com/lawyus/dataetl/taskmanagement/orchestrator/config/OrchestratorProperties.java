package com.lawyus.dataetl.taskmanagement.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.List;

/**
 * 编排器退避 / 超时阈值（{@code orchestrator-design.md §7.8}）。全部集中在 config-server 的
 * {@code config-repo/task-management-service.yml} 的 {@code etl.orchestrator} 段，代码不写死。
 *
 * <p>每个阈值都对应 §7.9 的一个 sweeper 判定分支。默认值即设计文档给的推荐值。
 */
@Component
@ConfigurationProperties(prefix = "etl.orchestrator")
public class OrchestratorProperties {

    /** 任务级自动重试次数上限（INV-7：{@code attempt_no} 上限 = maxAttempts + 1）。 */
    private int maxAttempts = 3;
    /** DISPATCHED 无心跳超时（T8/T9）。 */
    private Duration dispatchHeartbeatTimeout = Duration.ofSeconds(60);
    /** 软取消 ack 超时（T16 强制取消）。 */
    private Duration cancelAckTimeout = Duration.ofSeconds(90);
    /** QUEUED(SCHEDULED) 饥饿阈值（T32）。 */
    private Duration scheduledStarvationTimeout = Duration.ofMinutes(5);
    /** QUEUED(THROTTLED) 饥饿阈值（T32）。 */
    private Duration throttledStarvationTimeout = Duration.ofMinutes(30);
    /** PARTIAL_SUCCESS 驻留上限（T18）。 */
    private Duration partialSuccessRetention = Duration.ofHours(24);
    /** NEEDS_ATTENTION 无操作升级告警阈值（不自动迁移）。 */
    private Duration needsAttentionEscalation = Duration.ofDays(7);
    /** CREATED 驻留重放校验链阈值（T31）。 */
    private Duration createdTimeout = Duration.ofSeconds(5);
    /** VALIDATED 驻留内部错误阈值（T30）。 */
    private Duration validatedTimeout = Duration.ofSeconds(10);
    /** RUNNING 卡死兜底阈值（无历史 shard 耗时样本时使用，§7.9）。 */
    private Duration stallFallbackThreshold = Duration.ofMinutes(30);
    /** 任务级退避序列（T12/T21 用），默认 5m/15m/1h。 */
    private List<Duration> backoffTaskAttempts = List.of(
            Duration.ofMinutes(5), Duration.ofMinutes(15), Duration.ofHours(1));

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public Duration getDispatchHeartbeatTimeout() {
        return dispatchHeartbeatTimeout;
    }

    public void setDispatchHeartbeatTimeout(Duration dispatchHeartbeatTimeout) {
        this.dispatchHeartbeatTimeout = dispatchHeartbeatTimeout;
    }

    public Duration getCancelAckTimeout() {
        return cancelAckTimeout;
    }

    public void setCancelAckTimeout(Duration cancelAckTimeout) {
        this.cancelAckTimeout = cancelAckTimeout;
    }

    public Duration getScheduledStarvationTimeout() {
        return scheduledStarvationTimeout;
    }

    public void setScheduledStarvationTimeout(Duration scheduledStarvationTimeout) {
        this.scheduledStarvationTimeout = scheduledStarvationTimeout;
    }

    public Duration getThrottledStarvationTimeout() {
        return throttledStarvationTimeout;
    }

    public void setThrottledStarvationTimeout(Duration throttledStarvationTimeout) {
        this.throttledStarvationTimeout = throttledStarvationTimeout;
    }

    public Duration getPartialSuccessRetention() {
        return partialSuccessRetention;
    }

    public void setPartialSuccessRetention(Duration partialSuccessRetention) {
        this.partialSuccessRetention = partialSuccessRetention;
    }

    public Duration getNeedsAttentionEscalation() {
        return needsAttentionEscalation;
    }

    public void setNeedsAttentionEscalation(Duration needsAttentionEscalation) {
        this.needsAttentionEscalation = needsAttentionEscalation;
    }

    public Duration getCreatedTimeout() {
        return createdTimeout;
    }

    public void setCreatedTimeout(Duration createdTimeout) {
        this.createdTimeout = createdTimeout;
    }

    public Duration getValidatedTimeout() {
        return validatedTimeout;
    }

    public void setValidatedTimeout(Duration validatedTimeout) {
        this.validatedTimeout = validatedTimeout;
    }

    public Duration getStallFallbackThreshold() {
        return stallFallbackThreshold;
    }

    public void setStallFallbackThreshold(Duration stallFallbackThreshold) {
        this.stallFallbackThreshold = stallFallbackThreshold;
    }

    public List<Duration> getBackoffTaskAttempts() {
        return backoffTaskAttempts;
    }

    public void setBackoffTaskAttempts(List<Duration> backoffTaskAttempts) {
        this.backoffTaskAttempts = backoffTaskAttempts;
    }
}
