package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorMetrics;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
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
 * 派工泵（{@code orchestrator-design.md §7.2} / 方案 v2 WP-01）。
 *
 * <p>与 v1 的关键差异（P0-1）：
 * <ul>
 *   <li>扫描 {@code QUEUED/DISPATCHED/RUNNING} 中仍有 PENDING 分片的 run，<b>持续补派</b>，
 *       不再只派首批 {@code permitsPerRun} 片；</li>
 *   <li>令牌<b>尽量获取</b>（{@link TokenBucket#tryAcquireUpTo}），不足时部分派工而非整体饿死；</li>
 *   <li>派工消息等待 broker ack；失败保持 PENDING、不标 ASSIGNED；</li>
 *   <li>{@code QUEUED→DISPATCHED}（T5）只在 run 仍为 QUEUED 时发生一次。</li>
 * </ul>
 *
 * <p>事务边界：{@link #dispatchOne} 经自注入代理（{@code @Lazy}）调用，使"分片置 ASSIGNED + T5 迁移"原子。
 * Phase 1B 切最小 outbox 前，消息在事务内直投（失败抛异常回滚，分片保持 PENDING）。
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
            List<EtlTaskRun> runs = runRepository.findDispatchableRuns(
                    PageRequest.of(0, dispatchProperties.getPumpBatch()));
            for (EtlTaskRun run : runs) {
                self.dispatchOne(run);
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /** 单个 run 的派工（事务单元）：尽量补齐到 permitsPerRun 的在跑分片。 */
    @Transactional
    public void dispatchOne(EtlTaskRun run) {
        String runId = run.getRunId();
        long pending = checkpointRepository.countPendingShards(runId);
        if (pending <= 0) {
            return;   // 尚无分片计划或已全部派完
        }
        int desired = (int) Math.min(pending, dispatchProperties.getPermitsPerRun());
        int permits = tokenBucket.tryAcquireUpTo(desired);
        if (permits <= 0) {
            if (run.statusEnum() == EtlTaskStatus.QUEUED) {
                runRepository.markQueuedReason(runId, "THROTTLED");   // T6：非状态迁移
            }
            metrics.dispatchThrottled();
            return;
        }

        List<EtlTaskCheckpoint> shards =
                checkpointRepository.findPendingShards(runId, PageRequest.of(0, permits));
        int attempt = run.getAttemptNo() == null ? 1 : run.getAttemptNo();
        boolean firstQueued = run.statusEnum() == EtlTaskStatus.QUEUED;
        int dispatched = 0;
        int released = 0;
        for (EtlTaskCheckpoint shard : shards) {
            try {
                ShardDispatchCommand cmd = assembler.assemble(run, shard);
                publisher.publish(cmd);   // 等待 broker ack；失败抛异常
                // CAS：只有真正从 PENDING 转出才计成功；并发泵已派则归还令牌
                int updated = checkpointRepository.markAssigned(
                        run.getTaskId(), runId, shard.getShardId(), attempt);
                if (updated == 0) {
                    tokenBucket.release(1);
                    released++;
                    continue;
                }
                dispatched++;
                metrics.dispatchShardPublished();
                if (firstQueued) {
                    // T5：首片派出时 run 跃迁 QUEUED → DISPATCHED（仅一次）
                    TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                            .reason("DISPATCH").actor("orchestrator:dispatch").build();
                    orchestrator.transition(runId, EtlTaskStatus.QUEUED, EtlTaskStatus.DISPATCHED, ctx);
                    firstQueued = false;
                }
            } catch (GuardFailureException | IllegalStateTransitionException e) {
                log.warn("pump: run={} shard={} dispatch skipped: {}", runId, shard.getShardId(), e.getMessage());
                break;
            } catch (RuntimeException e) {
                // 组装/发送失败：分片保持 PENDING，下轮补派；不污染状态机
                log.error("pump: run={} shard={} dispatch failed, keep PENDING: {}",
                        runId, shard.getShardId(), e.getMessage(), e);
                break;
            }
        }
        int unused = permits - dispatched - released;
        if (unused > 0) {
            tokenBucket.release(unused);
        }
    }
}
