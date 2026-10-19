package com.lawyus.dataetl.connector.spi.jdbc;

import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.model.ColumnDef;
import com.lawyus.dataetl.connector.spi.model.SinkResult;
import com.lawyus.dataetl.connector.spi.model.TableSchema;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class JdbcSinkWriterSqlTest {

    private final JdbcSinkWriter writer = new JdbcSinkWriter(ConnectorType.POSTGRES, null);

    @Test
    @DisplayName("buildInsert 按列序生成占位符")
    void buildInsert() {
        String sql = writer.buildInsert("t_dest", List.of("id", "name", "age"));
        assertThat(sql).isEqualTo("INSERT INTO t_dest (id, name, age) VALUES (?, ?, ?)");
    }

    @Test
    @DisplayName("buildCreateTable 映射目标类型并强制 idempotency_key 主键")
    void buildCreateTable() {
        TableSchema schema = new TableSchema("t_dest", List.of(
                new ColumnDef("id", "int4", java.sql.Types.INTEGER, 0, false),
                new ColumnDef("name", "varchar", java.sql.Types.VARCHAR, 100, true)));
        assertThat(writer.buildCreateTable(schema))
                .isEqualTo("CREATE TABLE IF NOT EXISTS t_dest (id integer, name varchar(100), "
                        + "idempotency_key VARCHAR(64) PRIMARY KEY)");
    }

    @Test
    @DisplayName("buildUpsert 带 idempotency_key 时 PG 追加 ON CONFLICT DO NOTHING")
    void buildPostgresUpsert() {
        TableSchema schema = new TableSchema("t_dest", List.of(
                new ColumnDef("id", "int4", java.sql.Types.INTEGER, 0, false),
                new ColumnDef("idempotency_key", "varchar", java.sql.Types.VARCHAR, 64, false)));
        assertThat(writer.buildUpsert(schema, List.of("id", "idempotency_key")))
                .isEqualTo("INSERT INTO t_dest (id, idempotency_key) VALUES (?, ?) "
                        + "ON CONFLICT (idempotency_key) DO NOTHING");
    }

    @Test
    @DisplayName("buildUpsert 对几何列使用 ST_GeomFromEWKT")
    void buildGeometryUpsert() {
        TableSchema schema = new TableSchema("t_dest", List.of(
                new ColumnDef("geom", "geometry", java.sql.Types.OTHER, 0, true),
                new ColumnDef("idempotency_key", "varchar", java.sql.Types.VARCHAR, 64, false)));
        assertThat(writer.buildUpsert(schema, List.of("geom", "idempotency_key")))
                .isEqualTo("INSERT INTO t_dest (geom, idempotency_key) "
                        + "VALUES (ST_GeomFromEWKT(?), ?) ON CONFLICT (idempotency_key) DO NOTHING");
    }

    @Test
    @DisplayName("空批直接返回零计数、不取连接")
    void emptyBatch() {
        assertThat(writer.writeBatch(new TableSchema("t", List.of()), List.of()))
                .isEqualTo(SinkResult.zero());
    }

    @Test
    @DisplayName("SinkResult 分账：filtered/failed 都计入 rowsDropped，但失败另计")
    void combine() {
        SinkResult r = new SinkResult(3, 1, 2).combine(new SinkResult(4, 0, 5));
        assertThat(r).isEqualTo(new SinkResult(7, 1, 7));
        SinkResult explicit = new SinkResult(5, 2, 1, 3, List.of());
        assertThat(explicit.rowsDropped()).isEqualTo(3);
        assertThat(explicit.rowsFailed()).isEqualTo(1);
        assertThat(explicit.rowsDeduped()).isEqualTo(3);
    }
}
