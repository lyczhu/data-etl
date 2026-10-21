package com.lawyus.dataetl.taskmanagement.orchestrator.outbox;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Outbox 投递器（Phase 2C）：定时取 due 行投递 Kafka，成功后标 sent，失败按退避重排。
 *
 * <p>多实例用 Redisson 锁串行；投递采用 at-least-once，worker 以 {@code idempotency_key} 去重。
 * 已发送行每日清理（{@code retention-days<=0} 关闭）。
 */
@Component
@ConditionalOnProperty(prefix = "etl.outbox", name = "enabled", havingValue = "true")
public class OutboxPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxPoller.class);
    private static final String LOCK_KEY = "etl:outbox:poller";
    private static final long ACK_TIMEOUT_SECONDS = 5;

    private final OutboxRepository outboxRepository;
    private final OutboxProperties properties;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RedissonClient redisson;
    private final MeterRegistry meterRegistry;

    public OutboxPoller(OutboxRepository outboxRepository,
                        OutboxProperties properties,
                        KafkaTemplate<String, String> kafkaTemplate,
                        RedissonClient redisson,
                        MeterRegistry meterRegistry) {
        this.outboxRepository = outboxRepository;
        this.properties = properties;
        this.kafkaTemplate = kafkaTemplate;
        this.redisson = redisson;
        this.meterRegistry = meterRegistry;
        Gauge.builder("etl.outbox.pending", outboxRepository, r -> {
                    try {
                        return r.countPending();
                    } catch (RuntimeException e) {
                        return 0L;
                    }
                })
                .description("待投递 outbox 行数")
                .register(meterRegistry);
        Gauge.builder("etl.outbox.oldest.pending.age.seconds", outboxRepository, r -> {
                    try {
                        Timestamp oldest = r.oldestPendingCreatedAt();
                        return oldest == null ? 0.0
                                : (System.currentTimeMillis() - oldest.getTime()) / 1000.0;
                    } catch (RuntimeException e) {
                        return 0.0;
                    }
                })
                .description("最老待投递 outbox 行的年龄")
                .register(meterRegistry);
    }

    @Scheduled(fixedDelayString = "${etl.outbox.poll-interval-ms:1000}")
    public void poll() {
        RLock lock = redisson.getLock(LOCK_KEY);
        if (!lock.tryLock()) {
            return;
        }
        try {
            List<OutboxRow> rows = outboxRepository.findDue(properties.getBatchSize());
            for (OutboxRow row : rows) {
                deliver(row);
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void deliver(OutboxRow row) {
        try {
            kafkaTemplate.send(row.topic(), row.messageKey(), row.payload())
                    .get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
            outboxRepository.markSent(row.id());
            meterRegistry.counter("etl.outbox.sent").increment();
        } catch (Exception e) {
            int attempt = row.attemptNo() + 1;
            long delay = properties.backoffFor(attempt);
            outboxRepository.markFailed(row.id(), attempt, delay, e.getMessage());
            meterRegistry.counter("etl.outbox.send.failures").increment();
            if (attempt >= properties.getMaxAttempts()) {
                log.error("outbox: row={} reached max attempts={}, last error: {}",
                        row.id(), attempt, e.getMessage());
            } else {
                log.warn("outbox: row={} attempt={} failed, retry in {}s: {}",
                        row.id(), attempt, delay, e.getMessage());
            }
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /** 已发送行保留期清理；默认每日 03:30。 */
    @Scheduled(cron = "${etl.outbox.retention-cron:0 30 3 * * *}")
    public void cleanupSentRows() {
        if (properties.getRetentionDays() <= 0) {
            return;
        }
        try {
            int deleted = outboxRepository.deleteSentOlderThanDays(properties.getRetentionDays());
            if (deleted > 0) {
                meterRegistry.counter("etl.outbox.cleaned").increment(deleted);
                log.info("outbox retention: deleted {} sent rows older than {} days",
                        deleted, properties.getRetentionDays());
            }
        } catch (RuntimeException e) {
            log.warn("outbox retention failed: {}", e.getMessage());
        }
    }
}
