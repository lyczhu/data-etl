package com.lawyus.dataetl.worker.pipeline;

import java.util.Locale;

/**
 * 接入数据形态（用于挑选默认转换规则链）。对应方案 §7.3 E 的各源类型聚类。
 *
 * <p>Phase 1 真正可接入的是 {@link #JDBC}；其余为骨架预置的模拟档，接入对应源后按名启用。
 */
public enum DataProfile {
    /** 关系库直读（POSTGRES/MYSQL）：列已有类型，仅需日期归一与数值精度。 */
    JDBC,
    /** CSV/文本：所有列都是脏字符串，需修剪 + 日期归一 + 类型还原。 */
    CSV,
    /** File JSON / REST：弱类型，数字/布尔可能带引号，日期为字符串。 */
    JSON,
    /** Mongo 嵌套文档：同 CSV 的脏字符串问题，另需展平前的字段裁剪。 */
    MONGO,
    /** GeoJSON：几何列是 GeoJSON 文本，需转 WKT 入 PostGIS。 */
    GEOJSON;

    /** 宽松解析（大小写/前后空白不敏感），未知值回退 {@link #JDBC}。 */
    public static DataProfile parseOrDefault(String raw) {
        if (raw == null || raw.isBlank()) {
            return JDBC;
        }
        try {
            return valueOf(raw.trim().toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException unknown) {
            return JDBC;
        }
    }
}
