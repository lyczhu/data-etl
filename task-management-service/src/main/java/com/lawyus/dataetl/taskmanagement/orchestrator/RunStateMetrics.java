package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskDltRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * 控制面状态型指标（WP-15 / Gate 2）：
 *
 * <ul>
 *   <li>{@code etl.run.state.count{state}} —— 各状态 run 数；</li>
 *   <li>{@code etl.checkpoint.lag.seconds} —— 全局 checkpoint 最近写入距今秒数；</li>
 *   <li>{@code etl.dlt.unresolved.size} —— 全局未解决 DLT 数。</li>
 * </ul>
 *
 * <p>Gauge 在 Prometheus 抓取时求值，不做后台轮询；查询异常时返回 0，避免抓取失败放大故障。
 * 均为低基数（status 12 个枚举），不含 taskId/runId。
 */
@Component
public class RunStateMetrics {

    public RunStateMetrics(MeterRegistry registry,
                           EtlTaskRunRepository runRepository,
                           EtlTaskCheckpointRepository checkpointRepository,
                           EtlTaskDltRepository dltRepository) {
        for (EtlTaskStatus status : EtlTaskStatus.values()) {
            String state = status.name();
            Gauge.builder("etl.run.state.count", () -> {
                try {
                    return runRepository.countByStatus(state);
                } catch (RuntimeException e) {
                    return 0L;
                }
            }).tag("state", state).register(registry);
        }

        Gauge.builder("etl.checkpoint.lag.seconds", () -> {
            try {
                Date max = checkpointRepository.globalMaxUpdatedAt();
                return max == null ? 0d
                        : Math.max(0d, (System.currentTimeMillis() - max.getTime()) / 1000d);
            } catch (RuntimeException e) {
                return 0d;
            }
        }).register(registry);

        Gauge.builder("etl.dlt.unresolved.size", () -> {
            try {
                return dltRepository.countUnresolvedGlobal();
            } catch (RuntimeException e) {
                return 0L;
            }
        }).register(registry);
    }
}
