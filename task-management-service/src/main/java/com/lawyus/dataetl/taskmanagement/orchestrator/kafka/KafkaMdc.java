package com.lawyus.dataetl.taskmanagement.orchestrator.kafka;

import org.slf4j.MDC;

/**
 * Kafka 监听器的 MDC 作用域（WP-15）：taskId/runId/shardId/attemptNo 统一注入，
 * 让控制面日志也能按 run/shard 关联（worker 侧已有同名键）。
 */
final class KafkaMdc {

    private KafkaMdc() {
    }

    static void put(String taskId, String runId, String shardId, Integer attemptNo) {
        putIfPresent("taskId", taskId);
        putIfPresent("runId", runId);
        putIfPresent("shardId", shardId);
        putIfPresent("attemptNo", attemptNo == null ? null : String.valueOf(attemptNo));
    }

    static void putAttempt(Integer attemptNo) {
        putIfPresent("attemptNo", attemptNo == null ? null : String.valueOf(attemptNo));
    }

    static void clear() {
        MDC.remove("taskId");
        MDC.remove("runId");
        MDC.remove("shardId");
        MDC.remove("attemptNo");
    }

    private static void putIfPresent(String key, String value) {
        if (value != null && !value.isEmpty()) {
            MDC.put(key, value);
        }
    }
}
