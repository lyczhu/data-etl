package com.lawyus.dataetl.worker.pipeline.transform;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.worker.pipeline.RecordTransformer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * GeoJSON → WKT：把文本列里的 GeoJSON 几何（{@code Point}/{@code LineString}/{@code Polygon}/
 * {@code MultiPoint}/{@code MultiLineString}）转成 PostGIS 可直接入表的 WKT 字面量。
 * 面向 {@code FILE_GEOJSON} 源与 {@code PostGIS} 目标（方案 §7.3 E）。
 *
 * <p>非 GeoJSON 值（不以 {@code {"type"} 开头或解析失败）原样保留，不误伤普通文本。纯函数、可单测。
 */
public class GeoJsonToWktTransformer implements RecordTransformer {

    private final ObjectMapper mapper = new ObjectMapper();

    @Override
    public SourceRecord apply(SourceRecord record) {
        if (record == null || record.columns() == null) {
            return record;
        }
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : record.columns().entrySet()) {
            Object v = e.getValue();
            if (v instanceof String s && looksLikeGeoJson(s)) {
                String wkt = toWkt(s);
                out.put(e.getKey(), wkt == null ? v : wkt);
            } else {
                out.put(e.getKey(), v);
            }
        }
        return new SourceRecord(record.shardId(), record.pk(), out);
    }

    static boolean looksLikeGeoJson(String s) {
        String t = s.trim();
        return t.startsWith("{") && t.contains("\"type\"") && t.contains("\"coordinates\"");
    }

    /** 可见以便单测：解析失败返回 {@code null}（调用方保留原值）。 */
    String toWkt(String geoJson) {
        try {
            JsonNode node = mapper.readTree(geoJson);
            String type = node.path("type").asText(null);
            JsonNode coords = node.path("coordinates");
            if (type == null || coords.isMissingNode()) {
                return null;
            }
            switch (type) {
                case "Point":
                    return "POINT (" + point(coords) + ")";
                case "LineString":
                    return "LINESTRING (" + pointList(coords) + ")";
                case "MultiPoint":
                    return "MULTIPOINT (" + pointList(coords) + ")";
                case "Polygon":
                    return "POLYGON ((" + ringList(coords) + "))";
                case "MultiLineString":
                    return "MULTILINESTRING ((" + ringList(coords) + "))";
                default:
                    return null;
            }
        } catch (Exception parseFailure) {
            return null;
        }
    }

    private static String point(JsonNode xy) {
        return trimNum(xy.get(0).asDouble()) + " " + trimNum(xy.get(1).asDouble());
    }

    /** 一层点数组：{@code [[x,y],[x,y]]}。 */
    private static String pointList(JsonNode arr) {
        List<String> pts = new ArrayList<>();
        for (JsonNode p : arr) {
            pts.add(point(p));
        }
        return String.join(", ", pts);
    }

    /** 两层嵌套（Polygon/MultiLineString 的一级环集合）。 */
    private static String ringList(JsonNode arr) {
        List<String> rings = new ArrayList<>();
        for (JsonNode ring : arr) {
            rings.add(pointList(ring));
        }
        return String.join(", ", rings);
    }

    private static String trimNum(double d) {
        if (d == Math.rint(d) && !Double.isInfinite(d)) {
            return Long.toString((long) d);
        }
        return Double.toString(d);
    }
}
