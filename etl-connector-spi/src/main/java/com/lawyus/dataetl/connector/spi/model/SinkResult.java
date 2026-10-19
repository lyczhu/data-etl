package com.lawyus.dataetl.connector.spi.model;

/**
 * 一批写入的计数结果。口径与 {@code orchestrator-design.md §7.5} 对齐：
 * 对账等式 {@code rowsOut + rowsDropped = rowsIn}，{@code rowsDeduped} 不入等式（D-08）。
 *
 * @param rowsOut     目标库新增/更新生效的行数
 * @param rowsDropped 因 FATAL 分类被丢弃并应送 DLT 的行数
 * @param rowsDeduped {@code ON CONFLICT DO NOTHING} 命中数（重放/续跑），不参与对账
 */
public record SinkResult(long rowsOut, long rowsDropped, long rowsDeduped) {

    public static SinkResult zero() {
        return new SinkResult(0, 0, 0);
    }

    public SinkResult combine(SinkResult other) {
        if (other == null) {
            return this;
        }
        return new SinkResult(rowsOut + other.rowsOut,
                rowsDropped + other.rowsDropped,
                rowsDeduped + other.rowsDeduped);
    }
}
