package com.lawyus.dataetl.connector.spi.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TypeMappingTest {

    @Test
    @DisplayName("MySQL 源类型映射到 PostgreSQL 目标类型")
    void mysqlToPostgres() {
        assertThat(TypeMapping.toPostgres("int4", 0)).isEqualTo("integer");
        assertThat(TypeMapping.toPostgres("bigint", 0)).isEqualTo("bigint");
        assertThat(TypeMapping.toPostgres("varchar", 100)).isEqualTo("varchar(100)");
        assertThat(TypeMapping.toPostgres("decimal", 18)).isEqualTo("numeric(18)");
        assertThat(TypeMapping.toPostgres("datetime", 0)).isEqualTo("timestamp");
        assertThat(TypeMapping.toPostgres("longblob", 0)).isEqualTo("bytea");
    }

    @Test
    @DisplayName("空间类型统一映射为 geometry(Geometry,4326)")
    void geometryMapping() {
        assertThat(TypeMapping.toPostgres("geometry", 0)).isEqualTo("geometry(Geometry,4326)");
        assertThat(TypeMapping.toPostgres("geography", 0)).isEqualTo("geometry(Geometry,4326)");
        assertThat(TypeMapping.isGeometry("multipolygon")).isTrue();
        assertThat(TypeMapping.isGeometry("varchar")).isFalse();
    }

    @Test
    @DisplayName("未知类型保留原名（便于人工发现），空类型兜底 text")
    void fallbacks() {
        assertThat(TypeMapping.toPostgres("citext", 0)).isEqualTo("citext");
        assertThat(TypeMapping.toPostgres(null, 0)).isEqualTo("text");
        assertThat(TypeMapping.toPostgres("", 0)).isEqualTo("text");
    }

    @Test
    @DisplayName("反向映射到 MySQL：int4/integer、boolean、uuid、geometry 文本")
    void toMysql() {
        assertThat(TypeMapping.toMysql("int4", 0)).isEqualTo("int");
        assertThat(TypeMapping.toMysql("integer", 0)).isEqualTo("int");
        assertThat(TypeMapping.toMysql("boolean", 0)).isEqualTo("tinyint(1)");
        assertThat(TypeMapping.toMysql("uuid", 0)).isEqualTo("char(36)");
        assertThat(TypeMapping.toMysql("geometry", 0)).isEqualTo("geometry");
    }
}
