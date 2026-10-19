package com.lawyus.dataetl.worker.pipeline.transform;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.worker.pipeline.RecordTransformer;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * 字段裁剪/归一：面向源表加列、CSV 多余列、JSON 大小写混写等 schema 漂移场景。
 *
 * <ul>
 *   <li>{@code keepColumns} 非空 → 只保留这些列（大小写不敏感匹配），并按其顺序输出（稳定目标表列序）。</li>
 *   <li>{@code normalizeNames=true} → 列名统一为 snake_case（{@code UserID}/{@code user-name} → {@code user_id}），
 *       便于 JDBC 目标建表与 {@code ON CONFLICT} 幂等键对齐。</li>
 * </ul>
 *
 * <p>纯函数、可单测；被裁掉的列不改变行数（不丢行），因此不影响对账等式 {@code rowsOut+rowsDropped=rowsIn}。
 */
public class FieldPruneTransformer implements RecordTransformer {

    private final Set<String> keep;      // 原始名的小写键 → 保留判定；null 表示不裁剪
    private final boolean normalizeNames;

    public FieldPruneTransformer() {
        this(null, true);
    }

    public FieldPruneTransformer(Collection<String> keepColumns, boolean normalizeNames) {
        this.normalizeNames = normalizeNames;
        if (keepColumns != null && !keepColumns.isEmpty()) {
            Set<String> k = new LinkedHashSet<>();
            for (String c : keepColumns) {
                if (c != null) {
                    k.add(c.trim().toLowerCase(Locale.ROOT));
                }
            }
            this.keep = k.isEmpty() ? null : k;
        } else {
            this.keep = null;
        }
    }

    @Override
    public SourceRecord apply(SourceRecord record) {
        if (record == null || record.columns() == null) {
            return record;
        }
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        if (keep != null) {
            // 源列小写名 → 原始列名/值，供白名单大小写不敏感匹配
            Map<String, String> lowerToOriginalKey = new LinkedHashMap<>();
            Map<String, Object> lowerToValue = new LinkedHashMap<>();
            for (Map.Entry<String, Object> e : record.columns().entrySet()) {
                String lk = e.getKey().toLowerCase(Locale.ROOT);
                lowerToOriginalKey.putIfAbsent(lk, e.getKey());
                lowerToValue.putIfAbsent(lk, e.getValue());
            }
            for (String token : keep) {
                String origKey = lowerToOriginalKey.get(token);
                if (origKey != null) {
                    String outKey = normalizeNames ? snakeCase(origKey) : origKey;
                    out.put(outKey, lowerToValue.get(token));
                }
            }
        } else {
            for (Map.Entry<String, Object> e : record.columns().entrySet()) {
                String key = normalizeNames ? snakeCase(e.getKey()) : e.getKey();
                out.put(key, e.getValue());
            }
        }
        return new SourceRecord(record.shardId(), record.pk(), out);
    }

    /** 可见以便单测。 */
    static String snakeCase(String name) {
        String s = name.replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                .replaceAll("[\\s\\-]+", "_")
                .toLowerCase(Locale.ROOT)
                .replaceAll("_+", "_");
        if (s.startsWith("_")) {
            s = s.substring(1);
        }
        if (s.endsWith("_")) {
            s = s.substring(0, s.length() - 1);
        }
        return s;
    }
}
