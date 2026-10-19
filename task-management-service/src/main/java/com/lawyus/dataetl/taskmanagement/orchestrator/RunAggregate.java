package com.lawyus.dataetl.taskmanagement.orchestrator;

/**
 * 某 run 下所有分片 checkpoint 的聚合视图，用于收敛判定与 G-2/G-4 guard。
 *
 * <p>口径（{@code orchestrator-design.md §7.5}）：终局一律重算，results 消息只是触发器；
 * 对账等式为 {@code rows_out + rows_filtered + rows_failed + rows_deduped = rows_in}。
 * deduped 入等式是 WP-02 崩溃重放语义的直接推论：目标已提交而 checkpoint 未写时，
 * 重放会再次读入同一批行（rows_in 增长），幂等命中只增 rows_deduped，不增 rows_out。
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

    /** 业务规则过滤行数（WP-04，可进成功等式）。 */
    Long getRowsFiltered();

    /** 写入/解析失败行数（WP-04，>0 时禁止 SUCCEEDED）。 */
    Long getRowsFailed();

    /**
     * 幂等命中数（{@code ON CONFLICT DO NOTHING}）。崩溃重放时该批行会被再次读入 rows_in，
     * 因此必须计入对账等式，否则“已去重”的重放会被误判为丢数。
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

    /** 行数对账（G-4）：rows_out + rows_filtered + rows_failed + rows_deduped = rows_in。 */
    default boolean isBalanced() {
        long filtered = getRowsFiltered() == null ? getRowsDropped() : getRowsFiltered();
        long failed = getRowsFailed() == null ? 0L : getRowsFailed();
        long deduped = getRowsDeduped() == null ? 0L : getRowsDeduped();
        return getRowsOut() + filtered + failed + deduped == getRowsIn();
    }

    /** 无写入失败行（WP-04：有 failed 行不得 SUCCEEDED）。 */
    default boolean hasNoFailedRows() {
        return getRowsFailed() == null || getRowsFailed() == 0L;
    }
}
