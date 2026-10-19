package com.lawyus.dataetl.worker.pipeline.transform;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class GeoJsonToWktTransformerTest {

    private final GeoJsonToWktTransformer transformer = new GeoJsonToWktTransformer();

    @Test
    @DisplayName("Point/LineString/Polygon 转 WKT")
    void basicGeometries() {
        assertThat(transformer.toWkt("{\"type\":\"Point\",\"coordinates\":[116.4,39.9]}"))
                .isEqualTo("POINT (116.4 39.9)");
        assertThat(transformer.toWkt("{\"type\":\"LineString\",\"coordinates\":[[0,0],[1,1]]}"))
                .isEqualTo("LINESTRING (0 0, 1 1)");
        assertThat(transformer.toWkt("{\"type\":\"Polygon\",\"coordinates\":[[[0,0],[2,0],[2,2],[0,0]]]}"))
                .isEqualTo("POLYGON ((0 0, 2 0, 2 2, 0 0))");
    }

    @Test
    @DisplayName("MultiPolygon 三层嵌套正确加括号")
    void multiPolygon() {
        String geojson = "{\"type\":\"MultiPolygon\",\"coordinates\":["
                + "[[[0,0],[1,0],[1,1],[0,0]]],"
                + "[[[2,2],[3,2],[3,3],[2,2]]]]}";
        assertThat(transformer.toWkt(geojson))
                .isEqualTo("MULTIPOLYGON (((0 0, 1 0, 1 1, 0 0)), ((2 2, 3 2, 3 3, 2 2)))");
    }

    @Test
    @DisplayName("GeometryCollection 递归转换各子几何")
    void geometryCollection() {
        String geojson = "{\"type\":\"GeometryCollection\",\"geometries\":["
                + "{\"type\":\"Point\",\"coordinates\":[1,2]},"
                + "{\"type\":\"LineString\",\"coordinates\":[[0,0],[1,1]]}]}";
        assertThat(transformer.toWkt(geojson))
                .isEqualTo("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 1 1))");
    }

    @Test
    @DisplayName("非 GeoJSON 文本原样保留，不误伤普通字符串")
    void plainTextUntouched() {
        Map<String, Object> cols = new LinkedHashMap<>();
        cols.put("note", "{\"type\":\"Point\"}");   // 缺 coordinates，不是几何
        cols.put("name", "hello");
        SourceRecord out = transformer.apply(new SourceRecord("shard-0", 1L, cols));

        assertThat(out.columns().get("note")).isEqualTo("{\"type\":\"Point\"}");
        assertThat(out.columns().get("name")).isEqualTo("hello");
    }

    @Test
    @DisplayName("非法 GeoJSON 保留原值而不是写坏目标表")
    void malformedKeepsOriginal() {
        Map<String, Object> cols = new LinkedHashMap<>();
        cols.put("geom", "{\"type\":\"Point\",\"coordinates\":\"bad\"}");
        SourceRecord out = transformer.apply(new SourceRecord("shard-0", 1L, cols));

        assertThat(out.columns().get("geom")).isEqualTo("{\"type\":\"Point\",\"coordinates\":\"bad\"}");
    }
}
