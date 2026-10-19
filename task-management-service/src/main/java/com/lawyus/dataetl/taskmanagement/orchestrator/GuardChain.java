package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskDltRepository;
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
 * 守护链：先做 G-1（迁移表合法性），再对适用边求值 G-2..G-5 与 G-7（{@code orchestrator-design.md §6.1}）。
 *
 * <p>v2（WP-10）补齐：
 * <ul>
 *   <li>G-2 总和校验：{@code succeeded+failed+skipped == shard_total}；</li>
 *   <li>T19 {@code PARTIAL_SUCCESS→SUCCEEDED} 要求 {@code failed==0} 且无未解决 DLT；</li>
 *   <li>G-7 DLT_CLEAN：任何进入 {@code SUCCEEDED} 的边都要求未解决 DLT 为 0；</li>
 *   <li>G-4 还要求 {@code rows_failed==0}（坏行不得判成功）。</li>
 * </ul>
 *
 * <p>G-6（VALIDATE 校验链）由 {@code RunValidator} 在入口链完成，不在迁移边求值。
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

    // 进入 SUCCEEDED 的边：G-7 DLT_CLEAN
    private static final Set<AllowedTransitions.Edge> SUCCESS_EDGES = Set.of(
            new AllowedTransitions.Edge(RUNNING, SUCCEEDED),        // T10
            new AllowedTransitions.Edge(PARTIAL_SUCCESS, SUCCEEDED) // T19
    );

    private final EtlTaskCheckpointRepository checkpointRepository;
    private final EtlTaskDltRepository dltRepository;

    public GuardChain(EtlTaskCheckpointRepository checkpointRepository,
                      EtlTaskDltRepository dltRepository) {
        this.checkpointRepository = checkpointRepository;
        this.dltRepository = dltRepository;
    }

    /** G-1：迁移边必须在表内，否则协议被破坏（抛 {@link IllegalStateTransitionException}）。 */
    public void requireAllowed(EtlTaskStatus from, EtlTaskStatus to) {
        if (!AllowedTransitions.isAllowed(from, to)) {
            throw new IllegalStateTransitionException(from, to);
        }
    }

    /**
     * 对该边适用的 G-2..G-5 / G-7 求值。
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
            long succeeded = nz(a.getSucceeded());
            long failed = nz(a.getFailed());
            long skipped = nz(a.getSkipped());
            long shardTotal = run.getShardTotal() == null ? 0L : run.getShardTotal().longValue();
            if (from == RUNNING && (succeeded != shardTotal || failed != 0L || skipped != 0L)) {
                throw new GuardFailureException("G-4_BALANCE", "并非所有分片 SUCCEEDED");
            }
            if (from == PARTIAL_SUCCESS && failed != 0L) {
                throw new GuardFailureException("G-4_BALANCE", "仍有 FAILED 分片，不能 T19 改判 SUCCEEDED");
            }
            if (!a.isBalanced()) {
                throw new GuardFailureException("G-4_BALANCE",
                        "rows_out + rows_filtered + rows_failed + rows_deduped != rows_in (out=" + a.getRowsOut()
                                + ", filtered=" + a.getRowsFiltered() + ", failed=" + a.getRowsFailed()
                                + ", deduped=" + a.getRowsDeduped() + ", in=" + a.getRowsIn() + ")");
            }
            if (!a.hasNoFailedRows()) {
                throw new GuardFailureException("G-4_BALANCE",
                        "rows_failed=" + a.getRowsFailed() + " > 0，不得 SUCCEEDED");
            }
        }

        if (SUCCESS_EDGES.contains(edge) && dltRepository.countUnresolved(run.getRunId()) > 0) {
            throw new GuardFailureException("G-7_DLT_CLEAN",
                    "run " + run.getRunId() + " 仍有未解决 DLT 行");
        }

        if (from == RUNNING && to == PARTIAL_SUCCESS) {
            RunAggregate a = checkpointRepository.aggregate(run.getRunId());
            long succeeded = nz(a.getSucceeded());
            long failed = nz(a.getFailed());
            long skipped = nz(a.getSkipped());
            long shardTotal = run.getShardTotal() == null ? 0L : run.getShardTotal().longValue();
            if (failed < 1) {
                throw new GuardFailureException("G-2_PARTIAL", "无 FAILED 分片，不满足 PARTIAL_SUCCESS 前置");
            }
            if (succeeded + failed + skipped != shardTotal) {
                throw new GuardFailureException("G-2_PARTIAL",
                        "分片计数不等于计划数: " + succeeded + "+" + failed + "+" + skipped + " != " + shardTotal);
            }
        }
    }

    private boolean isQuiesced(String runId) {
        return checkpointRepository.countActiveShards(runId) == 0;
    }

    private static long nz(Long v) {
        return v == null ? 0L : v;
    }
}
