package com.lawyus.dataetl.worker.pipeline.transform;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.worker.pipeline.RecordTransformer;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 字符串修剪：对所有 String 列 {@code trim}，全空白 → {@code null}。面向 CSV/JSON/关系表常见的脏文本。
 */
public class StringTrimTransformer implements RecordTransformer {

    @Override
    public SourceRecord apply(SourceRecord record) {
        if (record == null || record.columns() == null) {
            return record;
        }
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : record.columns().entrySet()) {
            out.put(e.getKey(), trim(e.getValue()));
        }
        return new SourceRecord(record.shardId(), record.pk(), out);
    }

    static Object trim(Object v) {
        if (v instanceof String s) {
            String t = s.trim();
            return t.isEmpty() ? null : t;
        }
        return v;
    }
}
