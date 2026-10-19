package com.lawyus.dataetl.connector.spi.model;

import java.util.Collections;
import java.util.List;

/**
 * 一批写入的计数结果。口径与 {@code orchestrator-design.md §7.5} / 方案 v2 WP-04 对齐：
 *
 * <ul>
 *   <li>{@code rowsFiltered} —— 业务规则过滤（transformer 返回 {@code null}），可进成功等式；</li>
 *   <li>{@code rowsFailed} —— 写入/解析失败（已落 {@code etl_task_dlt}），&gt;0 时禁止 {@code SUCCEEDED}；</li>
 *   <li>{@code rowsDropped = rowsFiltered + rowsFailed} —— 兼容旧口径；对账等式为
 *       {@code rows_out + rows_filtered + rows_failed + rows_deduped = rows_in}；</li>
 *   <li>{@code rowsDeduped} —— {@code ON CONFLICT DO NOTHING} 命中数（重放/续跑）；
 *       重放会再次读入 rows_in，因此它必须计入对账等式。</li>
 * </ul>
 *
 * <p>保留 3 参构造以兼容既有调用（第二参视为业务过滤数）。
 */
public record SinkResult(long rowsOut,
                         long rowsFiltered,
                         long rowsFailed,
                         long rowsDeduped,
                         List<FailedRow> failures) {

    public SinkResult {
        failures = failures == null ? Collections.emptyList() : List.copyOf(failures);
    }

    /** 兼容旧 3 参构造：{@code rowsDropped} 按业务过滤处理。 */
    public SinkResult(long rowsOut, long rowsDropped, long rowsDeduped) {
        this(rowsOut, rowsDropped, 0, rowsDeduped, Collections.emptyList());
    }

    public static SinkResult zero() {
        return new SinkResult(0, 0, 0, 0, Collections.emptyList());
    }

    public long rowsDropped() {
        return rowsFiltered + rowsFailed;
    }

    public SinkResult combine(SinkResult other) {
        if (other == null) {
            return this;
        }
        List<FailedRow> merged = new java.util.ArrayList<>(failures);
        merged.addAll(other.failures);
        return new SinkResult(rowsOut + other.rowsOut,
                rowsFiltered + other.rowsFiltered,
                rowsFailed + other.rowsFailed,
                rowsDeduped + other.rowsDeduped,
                merged);
    }
}
