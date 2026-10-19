package com.lawyus.dataetl.taskmanagement.orchestrator;

/**
 * 某 run 下所有分片 checkpoint 的聚合视图，用于收敛判定与 G-2/G-4 guard。
 *
 * <p>口径（{@code orchestrator-design.md §7.5}）：终局一律重算，results 消息只是触发器；
 * 对账等式为 {@code rows_out + rows_dropped = rows_in}，{@code rows_deduped} 不入等式（D-08）。
 *
 * <p>各计数列由 {@code EtlTaskCheckpointRepository#aggregate} 的投影查询填充，无数据时为 0。
 */
public interface RunAggregate {

    Long getPending();

    Long getAssigned();

    Long getRunning();

    Long getSucceeded();

    Long getFailed();

    Long getSkipped();

    Long getRowsIn();

    Long getRowsOut();

    Long getRowsDropped();

    /**
     * 幂等命中数（{@code ON CONFLICT DO NOTHING}）。按 D-08 <b>不入</b>对账等式，
     * 仅作 {@code etl_task_run.rows_deduped} 的投影来源（§4.2）。
     */
    Long getRowsDeduped();

    /** 是否仍有分片未进入终态（PENDING/ASSIGNED/RUNNING），用于 §7.5 收敛门。 */
    default boolean hasUnconverged() {
        return getPending() + getAssigned() + getRunning() > 0;
    }

    /** quiesce（G-3）：不存在 ASSIGNED/RUNNING 的活跃分片。 */
    default boolean isQuiesced() {
        return getAssigned() + getRunning() == 0;
    }

    /** 行数对账（G-4）：rows_out + rows_dropped = rows_in。 */
    default boolean isBalanced() {
        return getRowsOut() + getRowsDropped() == getRowsIn();
    }
}
