package com.lawyus.dataetl.taskmanagement.orchestrator.audit;

import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskAuditRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.concurrent.TimeUnit;

/**
 * 审计保留期清理（Phase 2B）：默认每日 03:00 删除 {@code created_at < now - retention-days} 的行。
 *
 * <p>与其它 sweeper 一样用 Redisson 单实例锁；{@code retention-days <= 0} 时完全不清理（安全开关）。
 */
@Component
public class AuditRetentionSweeper {

    private static final Logger log = LoggerFactory.getLogger(AuditRetentionSweeper.class);
    private static final String LOCK_KEY = "etl:sweeper:audit-retention";
    private static final long DAY_MS = 24L * 60 * 60 * 1000;

    private final EtlTaskAuditRepository auditRepository;
    private final AuditProperties properties;
    private final RedissonClient redisson;
    private final MeterRegistry meterRegistry;

    public AuditRetentionSweeper(EtlTaskAuditRepository auditRepository,
                                 AuditProperties properties,
                                 RedissonClient redisson,
                                 MeterRegistry meterRegistry) {
        this.auditRepository = auditRepository;
        this.properties = properties;
        this.redisson = redisson;
        this.meterRegistry = meterRegistry;
    }

    @Scheduled(cron = "${etl.audit.retention-cron:0 0 3 * * *}")
    public void sweep() {
        if (properties.getRetentionDays() <= 0) {
            return;
        }
        RLock lock = redisson.getLock(LOCK_KEY);
        try {
            if (!lock.tryLock(0, 30, TimeUnit.SECONDS)) {
                return;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return;
        }
        try {
            Date cutoff = new Date(System.currentTimeMillis() - properties.getRetentionDays() * DAY_MS);
            int deleted = auditRepository.deleteOlderThan(cutoff);
            if (deleted > 0) {
                meterRegistry.counter("etl.audit.cleanup.deleted").increment(deleted);
                log.info("audit retention: deleted {} rows older than {}", deleted, cutoff);
            }
        } catch (RuntimeException e) {
            log.warn("audit retention failed: {}", e.getMessage());
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
