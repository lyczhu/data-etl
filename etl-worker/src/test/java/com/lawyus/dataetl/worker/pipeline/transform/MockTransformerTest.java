package com.lawyus.dataetl.worker.pipeline.transform;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class MockTransformerTest {

    private static SourceRecord rec(String shardId, Map<String, Object> cols) {
        return new SourceRecord(shardId, cols.get("id"), cols);
    }

    private static LinkedHashMap<String, Object> cols(Object... kv) {
        LinkedHashMap<String, Object> m = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            m.put((String) kv[i], kv[i + 1]);
        }
        return m;
    }

    @Test
    @DisplayName("日期归一：多种格式与 epoch 统一为 ISO，非日期原样保留")
    void dateNormalize() {
        assertThat(DateNormalizeTransformer.normalize("2026/01/02 03:04:05")).isEqualTo("2026-01-02 03:04:05");
        assertThat(DateNormalizeTransformer.normalize("02/01/2026")).isEqualTo("2026-01-02");
        assertThat(DateNormalizeTransformer.normalize("2026-01-02T03:04:05")).isEqualTo("2026-01-02 03:04:05");
        // epoch 毫秒/秒（UTC）
        assertThat(DateNormalizeTransformer.normalize("1735689600000")).isEqualTo("2025-01-01 00:00:00");
        assertThat(DateNormalizeTransformer.normalize("1735689600")).isEqualTo("2025-01-01 00:00:00");
        // 非日期文本/普通数字串不误伤
        assertThat(DateNormalizeTransformer.normalize("hello")).isEqualTo("hello");
        assertThat(DateNormalizeTransformer.normalize("12345")).isEqualTo("12345"); // 5 位，非 epoch
        assertThat(DateNormalizeTransformer.normalize(42)).isEqualTo(42);           // 非字符串
    }

    @Test
    @DisplayName("类型强制：数字串→Long/BigDecimal，布尔串→Boolean，其余保留")
    void typeCoercion() {
        assertThat(TypeCoercionTransformer.coerce("123")).isEqualTo(123L);
        assertThat(TypeCoercionTransformer.coerce("-4.50")).isEqualTo(new java.math.BigDecimal("-4.50"));
        assertThat(TypeCoercionTransformer.coerce("true")).isEqualTo(Boolean.TRUE);
        assertThat(TypeCoercionTransformer.coerce("abc")).isEqualTo("abc");
        assertThat(TypeCoercionTransformer.coerce(7)).isEqualTo(7);
    }

    @Test
    @DisplayName("字符串修剪：trim 且全空白→null，非字符串保留")
    void stringTrim() {
        assertThat(StringTrimTransformer.trim("  hi  ")).isEqualTo("hi");
        assertThat(StringTrimTransformer.trim("   ")).isNull();
        assertThat(StringTrimTransformer.trim(5)).isEqualTo(5);
    }

    @Test
    @DisplayName("GeoJSON→WKT：Point/Line/Polygon 转换，非几何保留原值")
    void geoJsonToWkt() {
        GeoJsonToWktTransformer t = new GeoJsonToWktTransformer();
        SourceRecord out = t.apply(rec("s1", cols(
                "geom", "{\"type\":\"Point\",\"coordinates\":[116.4,39.9]}",
                "line", "{\"type\":\"LineString\",\"coordinates\":[[0,0],[1,1]]}",
                "poly", "{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[2,0],[2,2],[0,0]]]}",
                "name", "普通文本")));
        assertThat(out.columns().get("geom")).isEqualTo("POINT (116.4 39.9)");
        assertThat(out.columns().get("line")).isEqualTo("LINESTRING (0 0, 1 1)");
        assertThat(out.columns().get("poly")).isEqualTo("POLYGON ((0 0, 2 0, 2 2, 0 0))");
        assertThat(out.columns().get("name")).isEqualTo("普通文本");
    }

    @Test
    @DisplayName("字段裁剪：白名单保序 + snake_case 归一列名")
    void fieldPrune() {
        FieldPruneTransformer t = new FieldPruneTransformer(List.of("UserID", "Name"), true);
        SourceRecord out = t.apply(rec("s1", cols(
                "UserID", 1, "Name", " tom ", "Extra", "x")));
        assertThat(out.columns().keySet()).containsExactly("user_id", "name");
        assertThat(FieldPruneTransformer.snakeCase("UserID")).isEqualTo("user_id");
        assertThat(FieldPruneTransformer.snakeCase("user-name")).isEqualTo("user_name");
        assertThat(FieldPruneTransformer.snakeCase("Order Item Id")).isEqualTo("order_item_id");
    }

    @Test
    @DisplayName("工厂：CSV 档整链跑通（修剪→日期→类型→数值），保序不丢行")
    void factoryCsvChain() {
        LinkedHashMap<String, Object> raw = cols(
                "id", " 7 ",
                "amount", " 12.3456 ",
                "joined", " 2026/01/02 ",
                "active", " true ");
        com.lawyus.dataetl.worker.pipeline.RecordTransformer chain =
                com.lawyus.dataetl.worker.pipeline.TransformerFactory.forProfile(
                        com.lawyus.dataetl.worker.pipeline.DataProfile.CSV, List.of(), 2);
        Map<String, Object> out = chain.apply(rec("s1", raw)).columns();
        assertThat(out.get("id")).isEqualTo(7L);
        assertThat(((java.math.BigDecimal) out.get("amount")).scale()).isEqualTo(2);
        assertThat(out.get("amount")).isEqualTo(new java.math.BigDecimal("12.35"));
        assertThat(out.get("joined")).isEqualTo("2026-01-02");
        assertThat(out.get("active")).isEqualTo(Boolean.TRUE);
        assertThat(out.keySet()).containsExactly("id", "amount", "joined", "active");
    }

    @Test
    @DisplayName("工厂：JDBC 档不做类型强制（列已带类型）")
    void factoryJdbcChain() {
        LinkedHashMap<String, Object> raw = cols("id", 7, "amount", 12.3456, "joined", "2026/01/02");
        com.lawyus.dataetl.worker.pipeline.RecordTransformer chain =
                com.lawyus.dataetl.worker.pipeline.TransformerFactory.forProfile(
                        com.lawyus.dataetl.worker.pipeline.DataProfile.JDBC, List.of(), 2);
        Map<String, Object> out = chain.apply(rec("s1", raw)).columns();
        assertThat(out.get("id")).isEqualTo(7);                     // 整数原样保留
        assertThat(out.get("amount")).isEqualTo(new java.math.BigDecimal("12.35"));
        assertThat(out.get("joined")).isEqualTo("2026-01-02");
    }
}
