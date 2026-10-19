package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import org.springframework.stereotype.Component;

import java.util.Set;

import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.CANCELLED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.DISPATCHED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.NEEDS_ATTENTION;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.PARTIAL_SUCCESS;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.QUEUED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RETRYING;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RUNNING;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.SUCCEEDED;

/**
 * 守护链：先做 G-1（迁移表合法性），再对适用边求值 G-2..G-5（{@code orchestrator-design.md §6.1 guard 表}）。
 *
 * <p>G-6（VALIDATE 校验链）与 G-7（DLT_CLEAN）依赖尚未在本切片落地的校验/DLT 基础设施
 * （分别在 dispatch/validate 流程与 Phase 2 的 DLT 表），此处预留 hook 并注明；其余 guard 已实现。
 *
 * <p>命名与 §10.1 指标标签 {@code etl.guard.failure.total{guard}} 对齐。
 */
@Component
public class GuardChain {

    // 需要 quiesce（G-3：无活跃 shard）的边
    private static final Set<AllowedTransitions.Edge> QUIESCE_EDGES = Set.of(
            new AllowedTransitions.Edge(DISPATCHED, QUEUED),        // T8
            new AllowedTransitions.Edge(RUNNING, RETRYING),         // T12
            new AllowedTransitions.Edge(RETRYING, DISPATCHED),      // T21
            new AllowedTransitions.Edge(NEEDS_ATTENTION, RUNNING),  // T25
            new AllowedTransitions.Edge(RUNNING, CANCELLED)         // T16（强制取消也需 quiesce 复核）
    );

    // 需要 attempt 预算（G-5）的自动重试边
    private static final Set<AllowedTransitions.Edge> ATTEMPT_EDGES = Set.of(
            new AllowedTransitions.Edge(DISPATCHED, QUEUED),        // T8
            new AllowedTransitions.Edge(RUNNING, RETRYING),         // T12
            new AllowedTransitions.Edge(PARTIAL_SUCCESS, RETRYING)  // T17
    );

    // 需要行数对账（G-4 BALANCE）的收敛边
    private static final Set<AllowedTransitions.Edge> BALANCE_EDGES = Set.of(
            new AllowedTransitions.Edge(RUNNING, SUCCEEDED),        // T10
            new AllowedTransitions.Edge(PARTIAL_SUCCESS, SUCCEEDED) // T19
    );

    private final EtlTaskCheckpointRepository checkpointRepository;

    public GuardChain(EtlTaskCheckpointRepository checkpointRepository) {
        this.checkpointRepository = checkpointRepository;
    }

    /** G-1：迁移边必须在表内，否则协议被破坏（抛 {@link IllegalStateTransitionException}）。 */
    public void requireAllowed(EtlTaskStatus from, EtlTaskStatus to) {
        if (!AllowedTransitions.isAllowed(from, to)) {
            throw new IllegalStateTransitionException(from, to);
        }
    }

    /**
     * 对该边适用的 G-2..G-5 求值。G-6/G-7 见类注释（本切片未接入校验链与 DLT 基础设施）。
     *
     * @param run 迁移前的 run 快照（含当前 status / attempt_no）
     */
    public void evaluate(EtlTaskRun run, EtlTaskStatus to, TransitionContext ctx) {
        EtlTaskStatus from = run.statusEnum();
        AllowedTransitions.Edge edge = new AllowedTransitions.Edge(from, to);

        if (ATTEMPT_EDGES.contains(edge) && run.getAttemptNo() >= ctx.maxAttempts()) {
            throw new GuardFailureException("G-5_ATTEMPTS",
                    "attempt_no=" + run.getAttemptNo() + " >= maxAttempts=" + ctx.maxAttempts());
        }

        if (QUIESCE_EDGES.contains(edge) && !isQuiesced(run.getRunId())) {
            throw new GuardFailureException("G-3_QUIESCE",
                    "run " + run.getRunId() + " 仍有 ASSIGNED/RUNNING 分片");
        }

        if (BALANCE_EDGES.contains(edge)) {
            RunAggregate a = checkpointRepository.aggregate(run.getRunId());
            long succeeded = a.getSucceeded() == null ? 0L : a.getSucceeded();
            long failed = a.getFailed() == null ? 0L : a.getFailed();
            long skipped = a.getSkipped() == null ? 0L : a.getSkipped();
            long shardTotal = run.getShardTotal() == null ? 0L : run.getShardTotal().longValue();
            if (from == RUNNING && (succeeded != shardTotal || failed != 0L || skipped != 0L)) {
                throw new GuardFailureException("G-4_BALANCE", "并非所有分片 SUCCEEDED");
            }
            if (!a.isBalanced()) {
                throw new GuardFailureException("G-4_BALANCE",
                        "rows_out + rows_dropped != rows_in (out=" + a.getRowsOut()
                                + ", dropped=" + a.getRowsDropped() + ", in=" + a.getRowsIn() + ")");
            }
        }

        if (from == RUNNING && to == PARTIAL_SUCCESS) {
            RunAggregate a = checkpointRepository.aggregate(run.getRunId());
            if (a.getFailed() < 1) {
                throw new GuardFailureException("G-2_PARTIAL", "无 FAILED 分片，不满足 PARTIAL_SUCCESS 前置");
            }
        }
    }

    private boolean isQuiesced(String runId) {
        return checkpointRepository.countActiveShards(runId) == 0;
    }
}
