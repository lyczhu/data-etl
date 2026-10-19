package com.lawyus.dataetl.taskmanagement.orchestrator.scheduler;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorMetrics;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.policy.SweeperPolicy;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * DISPATCHED 心跳超时 sweeper（§6.5 / §7.9，15s）。对应 T8 / T9 与"心跳丢失但 worker 在跑"的 RUNNING 补登记。
 *
 * <p>单实例执行（Redisson 锁），全部通过 {@link OrchestratorService#transition} 落状态，绝不自写 SQL 改 status。
 * 判定顺序：先 quiesce 检查，避免"心跳丢失但分片仍在跑"时盲目回退造成 shard 双执行（§7.9 注）。
 */
@Component
public class DispatchTimeoutSweeper {

    private static final Logger log = LoggerFactory.getLogger(DispatchTimeoutSweeper.class);
    private static final String LOCK_KEY = "etl:sweeper:dispatch-timeout";

    private final RedissonClient redisson;
    private final EtlTaskRunRepository runRepository;
    private final EtlTaskCheckpointRepository checkpointRepository;
    private final OrchestratorService orchestrator;
    private final OrchestratorProperties properties;
    private final OrchestratorMetrics metrics;

    public DispatchTimeoutSweeper(RedissonClient redisson,
                                  EtlTaskRunRepository runRepository,
                                  EtlTaskCheckpointRepository checkpointRepository,
                                  OrchestratorService orchestrator,
                                  OrchestratorProperties properties,
                                  OrchestratorMetrics metrics) {
        this.redisson = redisson;
        this.runRepository = runRepository;
        this.checkpointRepository = checkpointRepository;
        this.orchestrator = orchestrator;
        this.properties = properties;
        this.metrics = metrics;
    }

    @Scheduled(fixedRate = 15_000)
    public void sweep() {
        RLock lock = redisson.getLock(LOCK_KEY);
        if (!lock.tryLock()) {
            return;
        }
        try {
            Date now = new Date();
            Date cutoff = new Date(now.getTime() - properties.getDispatchHeartbeatTimeout().toMillis());
            for (EtlTaskRun run : runRepository.findDispatchedTimedOut(cutoff)) {
                handle(run, now);
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    private void handle(EtlTaskRun run, Date now) {
        boolean quiesced = checkpointRepository.countActiveShards(run.getRunId()) == 0;
        SweeperPolicy.DispatchTimeoutAction action =
                SweeperPolicy.dispatchTimeout(run.getAttemptNo(), properties.getMaxAttempts(), quiesced);

        EtlTaskStatus to;
        String reason;
        switch (action) {
            case RECOVER_RUNNING:
                // 心跳丢失但分片在跑：CAS 补登记 RUNNING（T7），不回退
                to = EtlTaskStatus.RUNNING;
                reason = "HEARTBEAT_LOST_RECOVER";
                break;
            case FALLBACK_QUEUED:
                to = EtlTaskStatus.QUEUED;   // T8
                reason = "NO_HEARTBEAT_TIMEOUT";
                break;
            default:                          // NEEDS_ATTENTION
                to = EtlTaskStatus.NEEDS_ATTENTION;   // T9
                reason = "NO_WORKER_HEARTBEAT";
                break;
        }
        TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                .reason(reason).actor("sweeper:dispatch-timeout").now(now).maxAttempts(properties.getMaxAttempts())
                .build();
        try {
            orchestrator.transition(run.getRunId(), EtlTaskStatus.DISPATCHED, to, ctx);
            metrics.sweeperAction("dispatch-timeout");
            log.info("DispatchTimeoutSweeper run={} action={} -> {}", run.getRunId(), action, to);
        } catch (GuardFailureException | IllegalStateTransitionException e) {
            log.warn("DispatchTimeoutSweeper skipped run={} action={} cause={}", run.getRunId(), action, e.getMessage());
        }
    }
}
