package com.lawyus.dataetl.worker.pipeline;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class NumberScaleTransformerTest {

    @Test
    @DisplayName("默认 scale<0：金额/坐标/ID 原样保留，不做静默四舍五入")
    void defaultKeepsValuesUnchanged() {
        BigDecimal amount = new BigDecimal("10.555");
        Double coordinate = 116.397128;
        Long id = 9007199254740993L;
        NumberScaleTransformer transformer = new NumberScaleTransformer();

        SourceRecord out = transformer.apply(record(amount, coordinate, id));

        assertThat(out.columns().get("amount")).isSameAs(amount);
        assertThat(out.columns().get("coordinate")).isEqualTo(coordinate);
        assertThat(out.columns().get("id")).isEqualTo(id);
    }

    @Test
    @DisplayName("显式 scale=2：BigDecimal/Double 按 HALF_UP 保留两位，整数不变")
    void explicitScaleRoundsOnlyDecimals() {
        NumberScaleTransformer transformer = new NumberScaleTransformer(2);

        SourceRecord out = transformer.apply(record(
                new BigDecimal("10.555"), 1.239, 7L));

        assertThat(out.columns().get("amount")).isEqualTo(new BigDecimal("10.56"));
        assertThat(out.columns().get("coordinate")).isEqualTo(new BigDecimal("1.24"));
        assertThat(out.columns().get("id")).isEqualTo(7L);
    }

    private static SourceRecord record(Object amount, Object coordinate, Object id) {
        Map<String, Object> cols = new LinkedHashMap<>();
        cols.put("amount", amount);
        cols.put("coordinate", coordinate);
        cols.put("id", id);
        return new SourceRecord("shard-0", 1L, cols);
    }
}
