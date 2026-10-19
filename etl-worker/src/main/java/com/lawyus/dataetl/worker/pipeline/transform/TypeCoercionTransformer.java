package com.lawyus.dataetl.worker.pipeline.transform;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.worker.pipeline.RecordTransformer;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 类型强制：把"看起来是数字/布尔"的字符串还原成对应类型（CSV/JSON/NoSQL 常把所有列读成 String）。
 * 整数 → {@link Long}，小数 → {@link BigDecimal}，true/false → {@link Boolean}；其余原样保留。
 * 应在 {@link DateNormalizeTransformer} 之后跑，避免把日期串误判。
 */
public class TypeCoercionTransformer implements RecordTransformer {

    private static final Pattern INT = Pattern.compile("-?\\d+");
    private static final Pattern DEC = Pattern.compile("-?\\d+\\.\\d+");

    @Override
    public SourceRecord apply(SourceRecord record) {
        if (record == null || record.columns() == null) {
            return record;
        }
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : record.columns().entrySet()) {
            out.put(e.getKey(), coerce(e.getValue()));
        }
        return new SourceRecord(record.shardId(), record.pk(), out);
    }

    static Object coerce(Object v) {
        if (!(v instanceof String s) || s.isEmpty()) {
            return v;
        }
        if (INT.matcher(s).matches()) {
            try {
                return Long.parseLong(s);
            } catch (NumberFormatException overflow) {
                return new BigDecimal(s);
            }
        }
        if (DEC.matcher(s).matches()) {
            return new BigDecimal(s);
        }
        if ("true".equalsIgnoreCase(s) || "false".equalsIgnoreCase(s)) {
            return Boolean.parseBoolean(s);
        }
        return v;
    }
}
