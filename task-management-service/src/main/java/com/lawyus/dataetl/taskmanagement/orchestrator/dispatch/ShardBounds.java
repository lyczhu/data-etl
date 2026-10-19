package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

/**
 * 一个待派工分片的整型主键闭区间 {@code [from, to]}（对齐 worker 侧 {@code ShardDispatch} 的 {@code pkFrom/pkTo}）。
 *
 * <p>设计缺口说明（{@code orchestrator-design.md §7.2} vs DDL 06）：checkpoint 表不持久化分片区间，
 * 派工消息的 {@code pkFrom/pkTo} 需由 {@link ShardRangeResolver} 现算。本切片提供与 SPI 解耦的默认解析器
 * （返回空区间，worker 依 {@code resumeCursor} 续跑）；真实 JDBC 分片规划随 validate/V-4 校验链落地。
 */
public record ShardBounds(Long from, Long to) {

    public static ShardBounds of(long from, long to) {
        return new ShardBounds(from, to);
    }

    /** 空解析结果（无区间）。 */
    public static ShardBounds empty() {
        return new ShardBounds(null, null);
    }

    public boolean isEmpty() {
        return from == null && to == null;
    }
}
