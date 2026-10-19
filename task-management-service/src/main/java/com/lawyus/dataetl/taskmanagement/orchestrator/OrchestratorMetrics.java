package com.lawyus.dataetl.taskmanagement.orchestrator;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

/**
 * 状态机迁移指标（{@code orchestrator-design.md §10.1}，延续 D-24 低基数原则）。
 *
 * <p>只使用 {@code from}/{@code to} 两个枚举名标签（基数 12×12，但仅 32 条实际出现）与
 * {@code guard} 名标签，绝不放 taskId/runId 之类高基数值。
 */
@Component
public class OrchestratorMetrics {

    private final MeterRegistry registry;

    public OrchestratorMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    /** transition 成功（CAS 命中）。 */
    public void transitionSuccess(EtlTaskStatus from, EtlTaskStatus to) {
        counter("etl.transition.total", from, to).increment();
    }

    /** CAS 返回 0：并发覆盖或重复投递，幂等丢弃。 */
    public void transitionIgnored(EtlTaskStatus from, EtlTaskStatus to) {
        counter("etl.transition.ignored.total", from, to).increment();
    }

    /** 非法迁移（迁移表外），协议被破坏。 */
    public void transitionIllegal(EtlTaskStatus from, EtlTaskStatus to) {
        counter("etl.transition.illegal.total", from, to).increment();
    }

    /** guard 求值失败，按 guard 名归类（G-2/G-3/G-4/G-5…）。 */
    public void guardFailure(String guard) {
        Counter.builder("etl.guard.failure.total")
                .tag("guard", guard)
                .register(registry)
                .increment();
    }

    /** 派工 pump：成功发出一片派工消息（§7.2，低基数，无标签）。 */
    public void dispatchShardPublished() {
        named("etl.dispatch.shard.published.total").increment();
    }

    /** 派工 pump：因令牌不足把 run 标 THROTTLED（T6）。 */
    public void dispatchThrottled() {
        named("etl.dispatch.throttled.total").increment();
    }

    /** 协议违例（如 RETRYING 窗口内收到活跃心跳，INV-5）。 */
    public void protocolViolation() {
        named("etl.protocol.violation.total").increment();
    }

    /** 消费到 DLT 行（触发 NEEDS_ATTENTION，§7.6）。 */
    public void dltReceived() {
        named("etl.dlt.received.total").increment();
    }

    /** 各 sweeper 实际推进了一次状态迁移（WP-15，带低基数 sweeper 标签）。 */
    public void sweeperAction(String sweeper) {
        Counter.builder("etl.sweeper.actions.total")
                .tag("sweeper", sweeper)
                .register(registry)
                .increment();
    }

    /** 影子比对不一致（WP-16，低基数，不改变状态）。 */
    public void shadowMismatch() {
        named("etl.shadow.compare.mismatch.total").increment();
    }

    private Counter named(String name) {
        return Counter.builder(name).register(registry);
    }

    private Counter counter(String name, EtlTaskStatus from, EtlTaskStatus to) {
        return Counter.builder(name)
                .tag("from", from.name())
                .tag("to", to.name())
                .register(registry);
    }
}
