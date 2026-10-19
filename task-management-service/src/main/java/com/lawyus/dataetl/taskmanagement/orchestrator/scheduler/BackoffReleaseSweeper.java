package com.lawyus.dataetl.taskmanagement.orchestrator.scheduler;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.policy.SweeperPolicy;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * 退避放行 + 排队饥饿 sweeper（§6.5 / §7.9，15s）。对应 T21（RETRYING 到期续派）与 T32（QUEUED 饥饿→人工）。
 *
 * <p>T5（QUEUED→DISPATCHED 首派）由 DispatchService 负责；本 sweeper 只在 QUEUED 驻留超阈值时把它踢进
 * NEEDS_ATTENTION，避免"永远排队"。
 */
@Component
public class BackoffReleaseSweeper {

    private static final Logger log = LoggerFactory.getLogger(BackoffReleaseSweeper.class);
    private static final String LOCK_KEY = "etl:sweeper:backoff-release";

    private final RedissonClient redisson;
    private final EtlTaskRunRepository runRepository;
    private final OrchestratorService orchestrator;
    private final OrchestratorProperties properties;

    public BackoffReleaseSweeper(RedissonClient redisson,
                                 EtlTaskRunRepository runRepository,
                                 OrchestratorService orchestrator,
                                 OrchestratorProperties properties) {
        this.redisson = redisson;
        this.runRepository = runRepository;
        this.orchestrator = orchestrator;
        this.properties = properties;
    }

    @Scheduled(fixedRate = 15_000)
    public void sweep() {
        RLock lock = redisson.getLock(LOCK_KEY);
        if (!lock.tryLock()) {
            return;
        }
        try {
            Date now = new Date();
            releaseDueRetries(now);
            flagStarvedQueues(now);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /** T21：RETRYING 退避到期 → DISPATCHED（复用同 run 的 checkpoint 续派）。 */
    private void releaseDueRetries(Date now) {
        for (EtlTaskRun run : runRepository.findRetryingDue(now)) {
            if (!SweeperPolicy.isDue(run.getNextAttemptAt(), now)) {
                continue;
            }
            TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                    .reason("BACKOFF_EXPIRED").actor("sweeper:backoff-release").now(now)
                    .maxAttempts(properties.getMaxAttempts()).build();
            safeTransition(run.getRunId(), EtlTaskStatus.RETRYING, EtlTaskStatus.DISPATCHED, ctx,
                    "releaseDueRetries");
        }
    }

    /** T32：QUEUED 驻留超阈值（SCHEDULED / THROTTLED 分别用各自阈值）→ NEEDS_ATTENTION。 */
    private void flagStarvedQueues(Date now) {
        long prefilter = Math.min(properties.getScheduledStarvationTimeout().toMillis(),
                properties.getThrottledStarvationTimeout().toMillis());
        Date cutoff = new Date(now.getTime() - prefilter);
        for (EtlTaskRun run : runRepository.findQueuedOlderThan(cutoff)) {
            if (!SweeperPolicy.isStarved(run.getQueuedReason(), run.getUpdatedAt(), now,
                    properties.getScheduledStarvationTimeout(), properties.getThrottledStarvationTimeout())) {
                continue;
            }
            TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                    .reason("DISPATCH_STARVED").actor("sweeper:backoff-release").now(now)
                    .maxAttempts(properties.getMaxAttempts()).build();
            safeTransition(run.getRunId(), EtlTaskStatus.QUEUED, EtlTaskStatus.NEEDS_ATTENTION, ctx,
                    "flagStarvedQueues");
        }
    }

    private void safeTransition(String runId, EtlTaskStatus from, EtlTaskStatus to, TransitionContext ctx, String op) {
        try {
            orchestrator.transition(runId, from, to, ctx);
            log.info("BackoffReleaseSweeper[{}] run={} {} -> {}", op, runId, from, to);
        } catch (GuardFailureException | IllegalStateTransitionException e) {
            log.warn("BackoffReleaseSweeper[{}] skipped run={} {} -> {} cause={}", op, runId, from, to, e.getMessage());
        }
    }
}
