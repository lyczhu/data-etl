package com.lawyus.dataetl.taskmanagement.orchestrator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConvergencePolicyTest {

    /** 测试用聚合视图（各计数字段直接构造）。 */
    private static RunAggregate agg(long pending, long assigned, long running, long succeeded,
                                    long failed, long skipped, long rowsIn, long rowsOut, long rowsDropped) {
        return new RunAggregate() {
            @Override public Long getPending() { return pending; }
            @Override public Long getAssigned() { return assigned; }
            @Override public Long getRunning() { return running; }
            @Override public Long getSucceeded() { return succeeded; }
            @Override public Long getFailed() { return failed; }
            @Override public Long getSkipped() { return skipped; }
            @Override public Long getRowsIn() { return rowsIn; }
            @Override public Long getRowsOut() { return rowsOut; }
            @Override public Long getRowsDropped() { return rowsDropped; }
        };
    }

    @Test
    @DisplayName("仍有活跃分片 → NOT_CONVERGED")
    void notConverged() {
        ConvergencePolicy.Outcome r = ConvergencePolicy.evaluate(agg(0, 1, 0, 2, 0, 0, 10, 10, 0), 3, false);
        assertThat(r).isEqualTo(ConvergencePolicy.Outcome.NOT_CONVERGED);
    }

    @Test
    @DisplayName("全 SUCCEEDED 且行数平 → SUCCEEDED（T10）")
    void succeededBalanced() {
        ConvergencePolicy.Outcome r = ConvergencePolicy.evaluate(agg(0, 0, 0, 3, 0, 0, 100, 98, 2), 3, false);
        assertThat(r).isEqualTo(ConvergencePolicy.Outcome.SUCCEEDED);
    }

    @Test
    @DisplayName("全 SUCCEEDED 但行数不平 → NEEDS_ATTENTION_BALANCE（G-4/T13）")
    void succeededImbalanced() {
        ConvergencePolicy.Outcome r = ConvergencePolicy.evaluate(agg(0, 0, 0, 3, 0, 0, 100, 90, 2), 3, false);
        assertThat(r).isEqualTo(ConvergencePolicy.Outcome.NEEDS_ATTENTION_BALANCE);
    }

    @Test
    @DisplayName("有 FAILED 其余成功/跳过 → PARTIAL_SUCCESS（T11）")
    void partial() {
        ConvergencePolicy.Outcome r = ConvergencePolicy.evaluate(agg(0, 0, 0, 1, 1, 1, 50, 30, 5), 3, false);
        assertThat(r).isEqualTo(ConvergencePolicy.Outcome.PARTIAL_SUCCESS);
    }

    @Test
    @DisplayName("全 FAILED → FAILED（T14）")
    void allFailed() {
        ConvergencePolicy.Outcome r = ConvergencePolicy.evaluate(agg(0, 0, 0, 0, 2, 0, 0, 0, 0), 2, false);
        assertThat(r).isEqualTo(ConvergencePolicy.Outcome.FAILED);
    }

    @Test
    @DisplayName("已收敛且有取消请求 → CANCELLED（T15，优先于其它收敛）")
    void cancelled() {
        ConvergencePolicy.Outcome r = ConvergencePolicy.evaluate(agg(0, 0, 0, 1, 0, 1, 40, 40, 0), 2, true);
        assertThat(r).isEqualTo(ConvergencePolicy.Outcome.CANCELLED);
    }

    @Test
    @DisplayName("dedup 跨 attempt：等式仍平（rows_deduped 不入等式，D-08）")
    void dedupKeepsBalance() {
        // rows_out(50)+rows_dropped(0)=rows_in(50)，dedup 不计 → 仍 SUCCEEDED
        ConvergencePolicy.Outcome r = ConvergencePolicy.evaluate(agg(0, 0, 0, 1, 0, 0, 50, 50, 0), 1, false);
        assertThat(r).isEqualTo(ConvergencePolicy.Outcome.SUCCEEDED);
    }

    @Test
    @DisplayName("无有效分片计划(shardTotal<=0) → 保守 NEEDS_ATTENTION_BALANCE")
    void noPlan() {
        ConvergencePolicy.Outcome r = ConvergencePolicy.evaluate(agg(0, 0, 0, 0, 0, 0, 0, 0, 0), 0, false);
        assertThat(r).isEqualTo(ConvergencePolicy.Outcome.NEEDS_ATTENTION_BALANCE);
    }
}
