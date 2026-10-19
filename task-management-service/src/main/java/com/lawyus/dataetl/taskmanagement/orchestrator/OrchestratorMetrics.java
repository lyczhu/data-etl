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

    private Counter counter(String name, EtlTaskStatus from, EtlTaskStatus to) {
        return Counter.builder(name)
                .tag("from", from.name())
                .tag("to", to.name())
                .register(registry);
    }
}
