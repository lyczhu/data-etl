package com.lawyus.dataetl.worker.pipeline;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 默认转换规则：浮点/定点列保留 {@code scale} 位小数（等价现 {@code DataTransformationService} 的默认规则，方案 §2.1 F2）。
 * 整数/其它类型原样保留。纯函数、可单测。
 */
public class NumberScaleTransformer implements RecordTransformer {

    private final int scale;

    public NumberScaleTransformer(int scale) {
        this.scale = Math.max(0, scale);
    }

    public NumberScaleTransformer() {
        this(2);
    }

    @Override
    public SourceRecord apply(SourceRecord record) {
        if (record == null || record.columns() == null) {
            return record;
        }
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : record.columns().entrySet()) {
            out.put(e.getKey(), scaleValue(e.getValue()));
        }
        return new SourceRecord(record.shardId(), record.pk(), out);
    }

    /** 可见以便单测。 */
    Object scaleValue(Object v) {
        if (v instanceof Double || v instanceof Float) {
            return BigDecimal.valueOf(((Number) v).doubleValue()).setScale(scale, RoundingMode.HALF_UP);
        }
        if (v instanceof BigDecimal) {
            return ((BigDecimal) v).setScale(scale, RoundingMode.HALF_UP);
        }
        return v;
    }
}
