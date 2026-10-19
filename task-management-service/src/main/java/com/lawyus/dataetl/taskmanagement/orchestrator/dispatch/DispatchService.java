package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorMetrics;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.DispatchProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.domain.PageRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 派工泵（{@code orchestrator-design.md §7.2}）：把 QUEUED run 的 PENDING 分片派成 {@code etl.task.dispatch.batch}
 * 消息，run 层仅首个分片跃迁 {@code QUEUED → DISPATCHED}（T5）；令牌不足标 {@code queued_reason=THROTTLED}（T6，非迁移）。
 *
 * <p>与 sweeper 同构：单实例（Redisson 锁）、只经 {@link OrchestratorService#transition} 落状态。
 * 受 {@link RoutingProperties#isNewWorker()} 影子开关门控——关闭时泵休眠，现网旧链路（{@code TaskScheduler}）不受影响。
 *
 * <p>事务边界：{@link #dispatchOne} 经自注入代理（{@code @Lazy}）调用，使"分片置 ASSIGNED + T5 迁移"原子；
 * 派工消息在事务内直投（Phase 1 弱 at-least-once，Phase 2 切 outbox）。
 */
@Component
public class DispatchService {

    private static final Logger log = LoggerFactory.getLogger(DispatchService.class);
    private static final String LOCK_KEY = "etl:sweeper:dispatch-pump";

    private final RedissonClient redisson;
    private final EtlTaskRunRepository runRepository;
    private final EtlTaskCheckpointRepository checkpointRepository;
    private final OrchestratorService orchestrator;
    private final TokenBucket tokenBucket;
    private final ShardDispatchAssembler assembler;
    private final DispatchPublisher publisher;
    private final DispatchProperties dispatchProperties;
    private final RoutingProperties routing;
    private final OrchestratorMetrics metrics;
    private final DispatchService self;

    public DispatchService(RedissonClient redisson,
                           EtlTaskRunRepository runRepository,
                           EtlTaskCheckpointRepository checkpointRepository,
                           OrchestratorService orchestrator,
                           TokenBucket tokenBucket,
                           ShardDispatchAssembler assembler,
                           DispatchPublisher publisher,
                           DispatchProperties dispatchProperties,
                           RoutingProperties routing,
                           OrchestratorMetrics metrics,
                           @Lazy DispatchService self) {
        this.redisson = redisson;
        this.runRepository = runRepository;
        this.checkpointRepository = checkpointRepository;
        this.orchestrator = orchestrator;
        this.tokenBucket = tokenBucket;
        this.assembler = assembler;
        this.publisher = publisher;
        this.dispatchProperties = dispatchProperties;
        this.routing = routing;
        this.metrics = metrics;
        this.self = self;
    }

    @Scheduled(fixedRate = 5_000)
    public void pump() {
        if (!routing.isNewWorker()) {
            return;   // 影子切换：新链路休眠，旧链路独占
        }
        RLock lock = redisson.getLock(LOCK_KEY);
        if (!lock.tryLock()) {
            return;
        }
        try {
            List<EtlTaskRun> queued = runRepository.findQueuedForDispatch(
                    PageRequest.of(0, dispatchProperties.getPumpBatch()));
            for (EtlTaskRun run : queued) {
                self.dispatchOne(run);
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /** 单个 QUEUED run 的派工（事务单元）。 */
    @Transactional
    public void dispatchOne(EtlTaskRun run) {
        String runId = run.getRunId();
        long pending = checkpointRepository.countPendingShards(runId);
        if (pending <= 0) {
            return;   // 尚无分片计划（validate/V-4 未落地），无可派
        }
        int permits = (int) Math.min(pending, dispatchProperties.getPermitsPerRun());
        if (!tokenBucket.tryAcquire(permits)) {
            runRepository.markQueuedReason(runId, "THROTTLED");   // T6：非状态迁移
            metrics.dispatchThrottled();
            return;
        }

        List<EtlTaskCheckpoint> shards =
                checkpointRepository.findPendingShards(runId, PageRequest.of(0, permits));
        boolean first = true;
        int dispatched = 0;
        for (EtlTaskCheckpoint shard : shards) {
            try {
                ShardDispatchCommand cmd = assembler.assemble(run, shard);
                publisher.publish(cmd);
                checkpointRepository.markAssigned(run.getTaskId(), runId, shard.getShardId());
                dispatched++;
                metrics.dispatchShardPublished();
                if (first) {
                    // T5：首片派出时 run 跃迁 QUEUED → DISPATCHED
                    TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                            .reason("DISPATCH").actor("orchestrator:dispatch").build();
                    orchestrator.transition(runId, EtlTaskStatus.QUEUED, EtlTaskStatus.DISPATCHED, ctx);
                    first = false;
                }
            } catch (GuardFailureException | IllegalStateTransitionException e) {
                log.warn("pump: run={} shard={} dispatch skipped: {}", runId, shard.getShardId(), e.getMessage());
                break;
            } catch (RuntimeException e) {
                // 组装/发送失败（源不可达等）：保持 QUEUED，未派分片留在 PENDING，下轮或 sweeper 重试
                log.error("pump: run={} shard={} dispatch failed, keep QUEUED: {}",
                        runId, shard.getShardId(), e.getMessage(), e);
                break;
            }
        }
        // 预占但未成功派发的许可即时归还（精确计，未超发）
        int unused = permits - dispatched;
        if (unused > 0) {
            tokenBucket.release(unused);
        }
    }
}
