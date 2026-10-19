package com.lawyus.dataetl.taskmanagement.orchestrator.scheduler;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.ErrorClass;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
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
 * 卡死 + 取消兜底 sweeper（§6.5 / §7.9，30s）。对应 T13（STALLED→NEEDS_ATTENTION）与 T16（取消 ack 超时强制 CANCELLED）。
 *
 * <p>卡死阈值本切片采用兜底值 {@code stall-fallback-threshold}（无每 shard 历史耗时样本时的 §7.9 缺省）；
 * "15×平均 shard 耗时"的动态阈值待 checkpoint 耗时统计落地后再细化。T16 必须叠 quiesce（否则可能打断在跑分片）。
 */
@Component
public class StallAndCancelSweeper {

    private static final Logger log = LoggerFactory.getLogger(StallAndCancelSweeper.class);
    private static final String LOCK_KEY = "etl:sweeper:stall-cancel";

    private final RedissonClient redisson;
    private final EtlTaskRunRepository runRepository;
    private final EtlTaskCheckpointRepository checkpointRepository;
    private final OrchestratorService orchestrator;
    private final OrchestratorProperties properties;

    public StallAndCancelSweeper(RedissonClient redisson,
                                 EtlTaskRunRepository runRepository,
                                 EtlTaskCheckpointRepository checkpointRepository,
                                 OrchestratorService orchestrator,
                                 OrchestratorProperties properties) {
        this.redisson = redisson;
        this.runRepository = runRepository;
        this.checkpointRepository = checkpointRepository;
        this.orchestrator = orchestrator;
        this.properties = properties;
    }

    @Scheduled(fixedRate = 30_000)
    public void sweep() {
        RLock lock = redisson.getLock(LOCK_KEY);
        if (!lock.tryLock()) {
            return;
        }
        try {
            Date now = new Date();
            detectStall(now);
            enforceCancel(now);
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /** T13：RUNNING 卡死 → NEEDS_ATTENTION。 */
    private void detectStall(Date now) {
        long threshold = properties.getStallFallbackThreshold().toMillis();
        Date cutoff = new Date(now.getTime() - threshold);
        for (EtlTaskRun run : runRepository.findRunningStalledCandidates(cutoff)) {
            Date ckMax = checkpointRepository.maxUpdatedAt(run.getRunId());
            // fallbackBaseline 用 started_at（进入 RUNNING 的基线），无心跳/无 checkpoint 活动时以此判卡死
            if (!SweeperPolicy.isStalled(run.getLastProgressTs(), ckMax, run.getStartedAt(), now,
                    properties.getStallFallbackThreshold())) {
                continue;
            }
            TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                    .reason("STALLED").actor("sweeper:stall-cancel").errorClass(ErrorClass.PERSISTENT)
                    .now(now).maxAttempts(properties.getMaxAttempts()).build();
            safeTransition(run.getRunId(), EtlTaskStatus.RUNNING, EtlTaskStatus.NEEDS_ATTENTION, ctx, "detectStall", null);
        }
    }

    /** T16：取消请求超 ack 窗口 ∧ quiesce → 强制 CANCELLED（forced=true）。 */
    private void enforceCancel(Date now) {
        long ack = properties.getCancelAckTimeout().toMillis();
        Date cutoff = new Date(now.getTime() - ack);
        for (EtlTaskRun run : runRepository.findCancelTimedOutCandidates(cutoff)) {
            if (!SweeperPolicy.isCancelTimedOut(run.getCancelRequestedAt(), now, properties.getCancelAckTimeout())) {
                continue;
            }
            if (checkpointRepository.countActiveShards(run.getRunId()) != 0) {
                continue; // 仍有在跑分片，等 worker chunk 边界自停，不强制
            }
            TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                    .reason("CANCEL_ACK_TIMEOUT").actor("sweeper:stall-cancel").forced(true)
                    .now(now).maxAttempts(properties.getMaxAttempts()).build();
            safeTransition(run.getRunId(), EtlTaskStatus.RUNNING, EtlTaskStatus.CANCELLED, ctx, "enforceCancel", "forced=true");
        }
    }

    private void safeTransition(String runId, EtlTaskStatus from, EtlTaskStatus to, TransitionContext ctx,
                                String op, String extra) {
        try {
            orchestrator.transition(runId, from, to, ctx);
            log.info("StallAndCancelSweeper[{}] run={} {} -> {} {}", op, runId, from, to, extra == null ? "" : extra);
        } catch (GuardFailureException | IllegalStateTransitionException e) {
            log.warn("StallAndCancelSweeper[{}] skipped run={} {} -> {} cause={}", op, runId, from, to, e.getMessage());
        }
    }
}
