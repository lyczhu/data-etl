package com.lawyus.dataetl.taskmanagement.orchestrator.target;

import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 每日孤儿标记（Phase 2E）：只把终态超过保留期的目标表标为 ORPHANED，<b>不删表</b>。
 * 真正删除由人工 {@code POST /api/admin/target-tables/cleanup?dryRun=false} 触发。
 */
@Component
@ConditionalOnProperty(prefix = "etl.target-table", name = "cleanup-enabled",
        havingValue = "true", matchIfMissing = true)
public class TargetTableSweeper {

    private static final Logger log = LoggerFactory.getLogger(TargetTableSweeper.class);
    private static final String LOCK_KEY = "etl:sweeper:target-table";

    private final TargetTableRegistry registry;
    private final RedissonClient redisson;

    public TargetTableSweeper(TargetTableRegistry registry, RedissonClient redisson) {
        this.registry = registry;
        this.redisson = redisson;
    }

    @Scheduled(cron = "${etl.target-table.orphan-cron:0 0 4 * * *}")
    public void sweep() {
        RLock lock = redisson.getLock(LOCK_KEY);
        if (!lock.tryLock()) {
            return;
        }
        try {
            int marked = registry.markOrphans();
            if (marked > 0) {
                log.info("target table sweeper: marked {} orphan candidates", marked);
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
