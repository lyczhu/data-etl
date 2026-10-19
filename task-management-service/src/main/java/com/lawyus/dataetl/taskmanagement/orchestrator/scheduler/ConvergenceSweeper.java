package com.lawyus.dataetl.taskmanagement.orchestrator.scheduler;

import com.lawyus.dataetl.taskmanagement.orchestrator.ConvergenceService;
import com.lawyus.dataetl.taskmanagement.orchestrator.RunAggregate;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 收敛 sweeper（WP-10 / P1-1，30s）：补齐“最后一条 results 丢失”的缺口。
 *
 * <p>对 DISPATCHED/RUNNING 且<b>无 PENDING/ASSIGNED/RUNNING 分片</b>的 run 重算 checkpoint 聚合，
 * 由 {@link ConvergenceService} 决定终局迁移。无 results 的正确数据也能进 SUCCEEDED。
 */
@Component
public class ConvergenceSweeper {

    private static final Logger log = LoggerFactory.getLogger(ConvergenceSweeper.class);
    private static final String LOCK_KEY = "etl:sweeper:convergence";

    private final RedissonClient redisson;
    private final EtlTaskRunRepository runRepository;
    private final EtlTaskCheckpointRepository checkpointRepository;
    private final ConvergenceService convergenceService;
    private final RoutingProperties routing;

    public ConvergenceSweeper(RedissonClient redisson,
                              EtlTaskRunRepository runRepository,
                              EtlTaskCheckpointRepository checkpointRepository,
                              ConvergenceService convergenceService,
                              RoutingProperties routing) {
        this.redisson = redisson;
        this.runRepository = runRepository;
        this.checkpointRepository = checkpointRepository;
        this.convergenceService = convergenceService;
        this.routing = routing;
    }

    @Scheduled(fixedRate = 30_000)
    public void sweep() {
        if (!routing.isNewWorker()) {
            return;
        }
        RLock lock = redisson.getLock(LOCK_KEY);
        if (!lock.tryLock()) {
            return;
        }
        try {
            for (EtlTaskRun run : runRepository.findActiveRuns()) {
                RunAggregate a = checkpointRepository.aggregate(run.getRunId());
                if (a.hasUnconverged()) {
                    continue;   // 仍有分片在跑/待派，交给 dispatcher/results
                }
                boolean acted = convergenceService.converge(run, a, "sweeper:convergence");
                if (acted) {
                    log.info("ConvergenceSweeper run={} converged without results", run.getRunId());
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }
}
