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
    @DisplayName("buildCreateTable 沿用源列类型 + IF NOT EXISTS")
    void buildCreateTable() {
        TableSchema schema = new TableSchema("t_dest", List.of(
                new ColumnDef("id", "int4", java.sql.Types.INTEGER, 0, false),
                new ColumnDef("name", "varchar", java.sql.Types.VARCHAR, 100, true)));
        assertThat(writer.buildCreateTable(schema))
                .isEqualTo("CREATE TABLE IF NOT EXISTS t_dest (id int4, name varchar)");
    }

    @Test
    @DisplayName("空批直接返回零计数、不取连接")
    void emptyBatch() {
        assertThat(writer.writeBatch(new TableSchema("t", List.of()), List.of()))
                .isEqualTo(SinkResult.zero());
    }

    @Test
    @DisplayName("SinkResult.combine 累加三个计数")
    void combine() {
        SinkResult r = new SinkResult(3, 1, 2).combine(new SinkResult(4, 0, 5));
        assertThat(r).isEqualTo(new SinkResult(7, 1, 7));
        assertThat(new SinkResult(1, 1, 1).rowsOut() + new SinkResult(1, 1, 1).rowsDropped())
                .isEqualTo(2);
    }
}
