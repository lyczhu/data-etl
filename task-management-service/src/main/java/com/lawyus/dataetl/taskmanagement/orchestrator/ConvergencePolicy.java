package com.lawyus.dataetl.taskmanagement.orchestrator;

/**
 * 收敛判定纯函数（{@code orchestrator-design.md §7.5} + 方案 v2 WP-04/WP-10）。
 *
 * <p>results 消息只是触发器，终局一律依 checkpoint 聚合重算（P1/D-16）。
 * 输入是当前 run 的 {@link RunAggregate}（worker 直写的 checkpoint 分桶）、{@code shardTotal} 与
 * {@code cancelRequested} 标志；输出应为哪个 run 级迁移。不含任何 DB/消息依赖，纯静态、可单测。
 *
 * <p>v2 修正：
 * <ul>
 *   <li>{@code SUCCEEDED} 要求 {@code rows_failed == 0} 且对账平衡（坏行不得静默判成功）；</li>
 *   <li>{@code PARTIAL_SUCCESS} 要求 {@code succeeded+failed+skipped == shard_total}（G-2 总和校验）；</li>
 *   <li>{@code rows_filtered} 与 {@code rows_deduped} 都参与对账；后者覆盖“目标已提交、checkpoint
 *       未写”的崩溃重放（同一批行被重读且全部幂等命中）。</li>
 * </ul>
 */
public final class ConvergencePolicy {

    public enum Outcome {
        NOT_CONVERGED,          // 仍有 PENDING/ASSIGNED/RUNNING 分片，等下一批
        CANCELLED,              // T15
        SUCCEEDED,              // T10
        PARTIAL_SUCCESS,        // T11
        FAILED,                 // T14（全部分片失败）
        NEEDS_ATTENTION_BALANCE // T13（行数对账不平或无合法收敛形态）
    }

    private ConvergencePolicy() {
    }

    public static Outcome evaluate(RunAggregate a, int shardTotal, boolean cancelRequested) {
        long pending = nz(a.getPending());
        long assigned = nz(a.getAssigned());
        long running = nz(a.getRunning());
        long succeeded = nz(a.getSucceeded());
        long failed = nz(a.getFailed());
        long skipped = nz(a.getSkipped());

        if (pending + assigned + running > 0) {
            return Outcome.NOT_CONVERGED;
        }
        if (cancelRequested) {
            return Outcome.CANCELLED;
        }
        if (shardTotal <= 0) {
            return Outcome.NEEDS_ATTENTION_BALANCE;   // 无有效分片计划，保守转人工
        }
        if (failed == shardTotal && succeeded == 0) {
            return Outcome.FAILED;
        }
        if (succeeded == shardTotal && failed == 0 && skipped == 0) {
            return a.isBalanced() && a.hasNoFailedRows()
                    ? Outcome.SUCCEEDED
                    : Outcome.NEEDS_ATTENTION_BALANCE;
        }
        if (failed >= 1) {
            // G-2 总和校验：已收敛时 succeeded+failed+skipped 必须等于计划分片数
            return (succeeded + failed + skipped == shardTotal)
                    ? Outcome.PARTIAL_SUCCESS
                    : Outcome.NEEDS_ATTENTION_BALANCE;
        }
        // 全 SKIPPED 等无 FAILED 的异常形态：保守转人工
        return Outcome.NEEDS_ATTENTION_BALANCE;
    }

    private static long nz(Long v) {
        return v == null ? 0L : v;
    }
}
