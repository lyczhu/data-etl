package com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 夜间对账扫描（Phase 2D）：近 {@code lookback-hours} 内结束的 SUCCEEDED/FAILED/CANCELLED run，
 * 每个 run 调 {@link ReconciliationService#reconcile} 一次；多实例用 Redisson 锁串行。
 */
@Component
@ConditionalOnProperty(prefix = "etl.reconciliation", name = "enabled",
        havingValue = "true", matchIfMissing = true)
public class ReconciliationScheduler {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationScheduler.class);
    private static final String LOCK_KEY = "etl:sweeper:reconciliation";

    private final ReconciliationService reconciliationService;
    private final ReconciliationProperties properties;
    private final JdbcTemplate jdbc;
    private final RedissonClient redisson;

    public ReconciliationScheduler(ReconciliationService reconciliationService,
                                   ReconciliationProperties properties,
                                   JdbcTemplate jdbc,
                                   RedissonClient redisson) {
        this.reconciliationService = reconciliationService;
        this.properties = properties;
        this.jdbc = jdbc;
        this.redisson = redisson;
    }

    @Scheduled(cron = "${etl.reconciliation.cron:0 30 2 * * *}")
    public void sweep() {
        if (!properties.isEnabled()) {
            return;
        }
        RLock lock = redisson.getLock(LOCK_KEY);
        if (!lock.tryLock()) {
            return;
        }
        try {
            List<String> runIds = jdbc.queryForList(
                    "SELECT run_id FROM etl_task_run WHERE status IN ('SUCCEEDED','FAILED','CANCELLED') "
                            + "AND COALESCE(finished_at, updated_at) >= "
                            + "now() - make_interval(hours => ?) "
                            + "ORDER BY COALESCE(finished_at, updated_at) DESC LIMIT ?",
                    String.class, properties.getLookbackHours(), properties.getBatchSize());
            int errors = 0;
            for (String runId : runIds) {
                try {
                    reconciliationService.reconcile(runId, false);
                } catch (RuntimeException e) {
                    errors++;
                    log.warn("reconciliation: run={} skipped: {}", runId, e.getMessage());
                }
            }
            log.info("reconciliation sweep: runs={} errors={}", runIds.size(), errors);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
