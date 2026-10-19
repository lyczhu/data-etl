package com.lawyus.dataetl.connector.spi.jdbc;

import java.util.Locale;

/**
 * 源列类型 → 目标列类型映射（方案 v2 WP-07）：Phase 1A 覆盖 MySQL→PostgreSQL 主场景，
 * 以及 PostgreSQL 自身的常用类型；未知类型按 {@code text} 兜底并保留原类型注释。
 *
 * <p>纯函数、可单测，不触库。
 */
public final class TypeMapping {

    private TypeMapping() {
    }

    /** MySQL/其它源类型名 → PostgreSQL 可建表类型。 */
    public static String toPostgres(String sourceType, int size) {
        if (sourceType == null || sourceType.isBlank()) {
            return "text";
        }
        String t = sourceType.toLowerCase(Locale.ROOT);
        // 去掉 MySQL 的 unsigned/zerofill 修饰
        t = t.replace(" unsigned", "").replace(" zerofill", "");
        if (t.startsWith("geometry") || t.startsWith("geography")) {
            return "geometry(Geometry,4326)";
        }
        switch (t) {
            case "tinyint":
            case "smallint":
            case "int2":
                return "smallint";
            case "mediumint":
            case "int":
            case "integer":
            case "int4":
                return "integer";
            case "bigint":
            case "int8":
                return "bigint";
            case "bit":
            case "bool":
            case "boolean":
                return "boolean";
            case "real":
            case "float4":
                return "real";
            case "double":
            case "double precision":
            case "float8":
                return "double precision";
            case "decimal":
            case "numeric":
            case "dec":
                return size > 0 ? "numeric(" + Math.min(size, 38) + ")" : "numeric";
            case "char":
            case "bpchar":
                return size > 0 ? "char(" + size + ")" : "char(1)";
            case "varchar":
            case "varchar2":
            case "nvarchar":
                return size > 0 && size <= 10485760 ? "varchar(" + size + ")" : "text";
            case "tinytext":
            case "text":
            case "mediumtext":
            case "longtext":
            case "json":
            case "jsonb":
                return "jsonb".equals(t) ? "jsonb" : ("json".equals(t) ? "jsonb" : "text");
            case "date":
                return "date";
            case "time":
                return "time";
            case "datetime":
            case "timestamp":
            case "timestamptz":
            case "timestamp with time zone":
                return "timestamp";
            case "blob":
            case "tinyblob":
            case "mediumblob":
            case "longblob":
            case "bytea":
            case "binary":
            case "varbinary":
                return "bytea";
            case "uuid":
                return "uuid";
            default:
                return t;
        }
    }

    /** 源类型名 → MySQL 可建表类型（反向主场景，供 MySQL 目标）。 */
    public static String toMysql(String sourceType, int size) {
        if (sourceType == null || sourceType.isBlank()) {
            return "longtext";
        }
        String t = sourceType.toLowerCase(Locale.ROOT);
        switch (t) {
            case "int2":
            case "smallint":
                return "smallint";
            case "int4":
            case "integer":
                return "int";
            case "int8":
            case "bigint":
                return "bigint";
            case "bool":
            case "boolean":
                return "tinyint(1)";
            case "float4":
            case "real":
                return "float";
            case "float8":
            case "double precision":
                return "double";
            case "numeric":
            case "decimal":
                return size > 0 ? "decimal(" + Math.min(size, 65) + ")" : "decimal(38,10)";
            case "varchar":
                return size > 0 && size <= 65535 ? "varchar(" + size + ")" : "longtext";
            case "text":
                return "longtext";
            case "timestamp":
            case "timestamptz":
            case "timestamp with time zone":
            case "datetime":
                return "datetime";
            case "date":
                return "date";
            case "time":
                return "time";
            case "bytea":
                return "longblob";
            case "jsonb":
            case "json":
                return "json";
            case "uuid":
                return "char(36)";
            default:
                return t;
        }
    }

    /** 是否空间类型（决定写入时是否包 {@code ST_GeomFromText}）。 */
    public static boolean isGeometry(String typeName) {
        if (typeName == null) {
            return false;
        }
        String t = typeName.toLowerCase(Locale.ROOT);
        return t.startsWith("geometry") || t.startsWith("geography") || t.equals("point")
                || t.equals("linestring") || t.equals("polygon") || t.equals("multipolygon");
    }
}
