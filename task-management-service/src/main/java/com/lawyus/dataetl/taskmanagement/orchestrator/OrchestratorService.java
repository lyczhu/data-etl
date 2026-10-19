package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.event.DomainEventPublisher;
import com.lawyus.dataetl.taskmanagement.orchestrator.event.EtlTaskTransitioned;
import com.lawyus.dataetl.taskmanagement.orchestrator.policy.BackoffSchedule;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.Optional;
import java.util.UUID;

import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.CANCELLED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.CREATED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.DISPATCHED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.FAILED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.NEEDS_ATTENTION;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.QUEUED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RETRYING;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RUNNING;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.SUCCEEDED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.SUPERSEDED;

/**
 * run 层状态机的唯一写入者，实现 {@link OrchestratorEngine}。
 *
 * <p>权威规范见 {@code docs/architecture/orchestrator-design.md §6.1}：
 * <ol>
 *   <li>{@link #transition} 是写 {@code etl_task_run.status} 的<b>唯一</b>路径（P2）；</li>
 *   <li>迁移表外的 from→to 抛 {@link IllegalStateTransitionException}（P5，与 CAS 失败严格区分）；</li>
 *   <li>CAS 返回 0 = 并发/重复投递，幂等忽略、返回真实状态、不抛异常（P4，至少一次语义）；</li>
 *   <li>成功后同事务刷新 {@code etl_task.status} 投影（INV-6），提交后外发迁移事件（§6.3）。</li>
 * </ol>
 *
 * <p>本切片（状态机内核）覆盖迁移表、guard、CAS、派生刷新与副作用主支；
 * audit 落表（INV-9）、shard 重置与四个 sweeper、DispatchService、Kafka 接线分别在后续切片补齐
 * （{@code orchestrator-design.md §13} 交付物 #6~#9）。
 */
@Service
public class OrchestratorService implements OrchestratorEngine {

    private static final Logger log = LoggerFactory.getLogger(OrchestratorService.class);

    private final EtlTaskRunRepository runRepository;
    private final EtlTaskCheckpointRepository checkpointRepository;
    private final EtlTaskRepository taskRepository;
    private final GuardChain guardChain;
    private final OrchestratorMetrics metrics;
    private final DomainEventPublisher eventPublisher;
    private final OrchestratorProperties properties;
    private final RunCounterProjector counterProjector;

    public OrchestratorService(EtlTaskRunRepository runRepository,
                               EtlTaskCheckpointRepository checkpointRepository,
                               EtlTaskRepository taskRepository,
                               GuardChain guardChain,
                               OrchestratorMetrics metrics,
                               DomainEventPublisher eventPublisher,
                               OrchestratorProperties properties,
                               RunCounterProjector counterProjector) {
        this.runRepository = runRepository;
        this.checkpointRepository = checkpointRepository;
        this.taskRepository = taskRepository;
        this.guardChain = guardChain;
        this.metrics = metrics;
        this.eventPublisher = eventPublisher;
        this.properties = properties;
        this.counterProjector = counterProjector;
    }

    /**
     * T1：新建 run（一次执行意图）。同一 task 若存在旧的终态 current run，先归档为
     * {@link EtlTaskStatus#SUPERSEDED}（T28/T29）。INV-1 由 DB 部分唯一索引兜底：
     * 已有非终态当前 run 时插入会触发唯一冲突，由上层翻译为 409 RUN_ALREADY_ACTIVE。
     *
     * @return 新建并落库的 run（状态 {@link EtlTaskStatus#CREATED}）
     */
    @Transactional
    public EtlTaskRun createRun(String taskId) {
        // 归档同 task 的旧终态 current run → SUPERSEDED
        runRepository.findFirstByTaskIdAndIsCurrentTrue(taskId).ifPresent(current -> {
            if (current.statusEnum().isTerminal()) {
                transition(current.getRunId(), current.statusEnum(), SUPERSEDED,
                        TransitionContext.builder(taskId).reason("SUPERSEDED_BY_NEW_RUN").actor("system").build());
            }
        });
        EtlTaskRun run = EtlTaskRun.newRun(UUID.randomUUID().toString(), taskId, 1);
        EtlTaskRun saved = runRepository.save(run);
        taskRepository.refreshDerivedStatus(taskId, CREATED.name());
        log.info("transition run={} from=(none) to={} reason=CREATE_TASK", saved.getRunId(), CREATED);
        return saved;
    }

    /**
     * 执行一次受守护的状态迁移（{@link OrchestratorEngine#transition}）。
     */
    @Override
    @Transactional
    public EtlTaskRun transition(String runId, EtlTaskStatus from, EtlTaskStatus to, TransitionContext ctx) {
        // G-1：迁移表合法性——非法即协议破坏，先于任何 DB 访问抛出并计数
        try {
            guardChain.requireAllowed(from, to);
        } catch (IllegalStateTransitionException e) {
            metrics.transitionIllegal(from, to);
            throw e;
        }

        EtlTaskRun run = runRepository.findByRunId(runId)
                .orElseThrow(() -> new IllegalArgumentException("ETL run not found: " + runId));

        // 期望 from 与库中实际状态不符：并发/重放导致，幂等忽略
        if (run.statusEnum() != from) {
            metrics.transitionIgnored(from, to);
            log.warn("transition ignored (stale from): run={} actual={} requested={} -> {}",
                    runId, run.getStatus(), from, to);
            return run;
        }

        // G-2..G-5：guard 求值，失败计 guard 数并抛出，事务回滚（不落任何状态）
        try {
            guardChain.evaluate(run, to, ctx);
        } catch (GuardFailureException gf) {
            metrics.guardFailure(gf.getGuard());
            log.warn("guard failed: run={} {} -> {} guard={} msg={}",
                    runId, from, to, gf.getGuard(), gf.getMessage());
            throw gf;
        }

        // P4：CAS 写状态。返回 0 = 并发已被覆盖 → 幂等丢弃，返回真实状态
        int updated = runRepository.casStatus(runId, from.name(), to.name());
        if (updated == 0) {
            metrics.transitionIgnored(from, to);
            return runRepository.findByRunId(runId).orElse(run);
        }

        applySideEffects(run, from, to, ctx);

        // INV-6：task.status 恒等于 current run.status，单向刷新
        if (ctx.taskId() != null) {
            taskRepository.refreshDerivedStatus(ctx.taskId(), to.name());
        }

        metrics.transitionSuccess(from, to);
        log.info("transition run={} from={} to={} reason={}", runId, from, to, ctx.reason());

        // §6.3 Phase 1：提交后外发（Kafka 投递由接线切片的 AFTER_COMMIT 监听器承接）
        eventPublisher.publishAfterCommit(new EtlTaskTransitioned(runId, ctx.taskId(), from, to, ctx));

        return runRepository.findByRunId(runId).orElse(run);
    }

    /**
     * 同一迁移事务内的副作用（时间戳、attempt、reason、计数快照）。不改 status（status 已由 CAS 落定）。
     * shard 重置、audit 落表、DLT 关联等依赖额外基础设施的副作用留待后续切片。
     *
     * <p>凡“结果已定局”的目标态都顺手做一次计数投影（{@link RunCounterProjector}）：
     * results 驱动的收敛已在监听器里投过，这里的重投是为了覆盖
     * <b>没有 results 消息的终局</b>（T9/T13/T16 由 sweeper 直接驱动），否则 run 行计数会停在中间值。
     */
    private void applySideEffects(EtlTaskRun run, EtlTaskStatus from, EtlTaskStatus to, TransitionContext ctx) {
        String runId = run.getRunId();
        Date now = ctx.now();
        switch (to) {
            case DISPATCHED:
                runRepository.markDispatchedAt(runId, now);
                break;
            case RUNNING:
                // T7 首次心跳：started_at 仅首次写入
                runRepository.markStartedIfAbsent(runId, now);
                break;
            case QUEUED:
                if (from == DISPATCHED) {
                    // T8 心跳超时回退 + attempt_no+1 + 退避
                    runRepository.incrementAttemptAndSchedule(runId, nextAttemptAt(now, run.getAttemptNo()));
                } else if (from == NEEDS_ATTENTION) {
                    // T24 人工 /retry：重置失败/跳过分片并推进 fencing，不排退避
                    int nextAttempt = nextAttempt(run);
                    runRepository.incrementAttemptAndSchedule(runId, null);
                    runRepository.markQueuedReason(runId, "SCHEDULED");
                    checkpointRepository.resetRetryableShards(runId, nextAttempt);
                } else {
                    runRepository.markQueuedReason(runId, "SCHEDULED");
                }
                break;
            case RETRYING:
                // T12/T17：attempt_no+1，排定退避到期时间；FAILED 分片重置为 PENDING，从 last_cursor 续跑
                int retryAttempt = nextAttempt(run);
                runRepository.incrementAttemptAndSchedule(runId, nextAttemptAt(now, run.getAttemptNo()));
                checkpointRepository.resetFailedShards(runId, retryAttempt);
                break;
            case NEEDS_ATTENTION:
                runRepository.markFinished(runId, null, summarize(ctx, from, to));
                counterProjector.project(runId);
                break;
            case PARTIAL_SUCCESS:
                // 收敛但带失败：同样落一版计数快照（§12.2 校验 2 要求 run 行与 checkpoint 一致）
                counterProjector.project(runId);
                break;
            case SUCCEEDED:
            case FAILED:
            case CANCELLED:
                runRepository.markFinished(runId, now,
                        to == FAILED || to == CANCELLED ? summarize(ctx, from, to) : null);
                counterProjector.project(runId);
                break;
            case SUPERSEDED:
                runRepository.clearCurrent(runId);
                break;
            default:
                // CREATED/VALIDATED：本切片无额外副作用
                break;
        }
    }

    private String summarize(TransitionContext ctx, EtlTaskStatus from, EtlTaskStatus to) {
        String ec = ctx.errorClass() == null ? "" : ("[" + ctx.errorClass().name() + "] ");
        return ec + from + "->" + to + ": " + (ctx.reason() == null ? "" : ctx.reason());
    }

    /** 下一次 attempt 序号（当前 +1，空视为 1）。 */
    private static int nextAttempt(EtlTaskRun run) {
        return (run.getAttemptNo() == null ? 1 : run.getAttemptNo()) + 1;
    }

    /** 退避：从集中配置 {@code etl.orchestrator.backoff-task-attempts} 取档（§7.8，代码不写死）。 */
    private Date nextAttemptAt(Date now, int attemptNo) {
        return BackoffSchedule.nextAttemptAt(properties.getBackoffTaskAttempts(), attemptNo, now);
    }

    /** 便捷读侧方法，供上层/测试查询当前 run。 */
    public Optional<EtlTaskRun> currentRun(String taskId) {
        return runRepository.findFirstByTaskIdAndIsCurrentTrue(taskId);
    }
}
