package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.connector.spi.CloseableIterator;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.jdbc.JdbcSinkWriter;
import com.lawyus.dataetl.connector.spi.jdbc.JdbcSourceConnector;
import com.lawyus.dataetl.connector.spi.model.ColumnDef;
import com.lawyus.dataetl.connector.spi.model.Shard;
import com.lawyus.dataetl.connector.spi.model.ShardParams;
import com.lawyus.dataetl.connector.spi.model.ShardPlan;
import com.lawyus.dataetl.connector.spi.model.SinkResult;
import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.connector.spi.model.SourceSpec;
import com.lawyus.dataetl.connector.spi.model.TableSchema;
import com.lawyus.dataetl.worker.pipeline.IdempotencyKeys;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2A：MySQL 源 → PostgreSQL 目标的真库回归（Gate 1 遗留项）。
 *
 * <p>走的是 worker 真实使用的 SPI 路径：{@link JdbcSourceConnector} 读 MySQL 分片，
 * {@link JdbcSinkWriter} 写 PG 目标表（含 {@code idempotency_key} 幂等键）。
 * 基础设施缺失时跳过（{@code -Pit} 下也不把构建染红）。
 */
@Tag("it")
class MySqlToPostgresSinkIT {

    private static final String SOURCE_TABLE = "it_mysql_order";
    private static final String TARGET_TABLE = "it_pg_from_mysql";
    private static final int ROWS = 20;

    @BeforeAll
    static void infra() {
        ItInfra.assumeMySqlUp();
        ItInfra.assumeInfrastructureUpAndBootstrap();
    }

    @Test
    @DisplayName("MySQL 20 行 → PG：行数/主键 min-max-sum 一致，重放全部命中幂等键")
    void mySqlSourceToPostgresSink_isConsistentAndIdempotent() throws Exception {
        Class.forName(ItInfra.MYSQL_DRIVER);
        seedMySqlSource();
        try {
            SourceSpec sourceSpec = new SourceSpec(ConnectorType.MYSQL, ItInfra.MYSQL_URL,
                    ItInfra.MYSQL_USER, ItInfra.MYSQL_PASSWORD, ItInfra.MYSQL_DRIVER, SOURCE_TABLE, 30);
            JdbcSourceConnector source = new JdbcSourceConnector(ConnectorType.MYSQL);

            TableSchema sourceSchema = source.readSchema(sourceSpec);
            TableSchema targetSchema = withIdempotencyKey(TARGET_TABLE, sourceSchema);
            JdbcSinkWriter sink = new JdbcSinkWriter(ConnectorType.POSTGRES, MySqlToPostgresSinkIT::pgConnection);

            try (Connection pg = pgConnection(); Statement st = pg.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + TARGET_TABLE);
            }
            sink.ensureTargetTable(targetSchema,
                    new SourceSpec(ConnectorType.POSTGRES, ItInfra.IT_DB_URL, ItInfra.PG_USER,
                            ItInfra.PG_PASSWORD, "org.postgresql.Driver", TARGET_TABLE));

            ShardPlan plan = source.planShards(sourceSpec, new ShardParams(8, 100, 3));
            assertThat(plan.shardCount()).isGreaterThanOrEqualTo(1);
            assertThat(plan.estimatedRows()).isEqualTo(ROWS);

            List<SourceRecord> all = readAll(source, sourceSpec, plan);
            assertThat(all).hasSize(ROWS);

            SinkResult first = sink.writeBatch(targetSchema, all);
            assertThat(first.rowsOut()).isEqualTo(ROWS);
            assertThat(first.rowsFailed()).isZero();
            assertThat(first.rowsDeduped()).isZero();

            SinkResult replay = sink.writeBatch(targetSchema, all);
            assertThat(replay.rowsOut()).isZero();
            assertThat(replay.rowsDeduped()).isEqualTo(ROWS);

            try (Connection pg = pgConnection(); Statement st = pg.createStatement()) {
                try (ResultSet rs = st.executeQuery("SELECT COUNT(*), MIN(id), MAX(id), SUM(id) FROM "
                        + TARGET_TABLE)) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong(1)).isEqualTo(ROWS);
                    assertThat(rs.getLong(2)).isEqualTo(1L);
                    assertThat(rs.getLong(3)).isEqualTo(ROWS);
                    assertThat(rs.getLong(4)).isEqualTo((long) ROWS * (ROWS + 1) / 2);
                }
                try (ResultSet rs = st.executeQuery("SELECT COUNT(DISTINCT idempotency_key) FROM "
                        + TARGET_TABLE)) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong(1)).isEqualTo(ROWS);
                }
            }
        } finally {
            cleanupMySql();
            try (Connection pg = pgConnection(); Statement st = pg.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + TARGET_TABLE);
            }
        }
    }

    private static void seedMySqlSource() throws Exception {
        try (Connection my = DriverManager.getConnection(ItInfra.MYSQL_URL,
                ItInfra.MYSQL_USER, ItInfra.MYSQL_PASSWORD);
             Statement st = my.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE);
            st.execute("CREATE TABLE " + SOURCE_TABLE + " ("
                    + "id BIGINT PRIMARY KEY, "
                    + "name VARCHAR(64), "
                    + "amount DECIMAL(10,2), "
                    + "created_at DATETIME(3))");
        }
        try (Connection my = DriverManager.getConnection(ItInfra.MYSQL_URL,
                ItInfra.MYSQL_USER, ItInfra.MYSQL_PASSWORD);
             PreparedStatement ps = my.prepareStatement("INSERT INTO " + SOURCE_TABLE
                     + " (id, name, amount, created_at) VALUES (?, ?, ?, ?)")) {
            for (int i = 1; i <= ROWS; i++) {
                ps.setLong(1, i);
                ps.setString(2, "mysql-" + i);
                ps.setBigDecimal(3, new BigDecimal("10.50"));
                ps.setTimestamp(4, Timestamp.valueOf("2026-02-01 00:00:00"));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    private static List<SourceRecord> readAll(JdbcSourceConnector source, SourceSpec spec, ShardPlan plan) {
        List<SourceRecord> all = new ArrayList<>(ROWS);
        for (Shard shard : plan.shards()) {
            try (CloseableIterator<SourceRecord> it = source.open(shard, spec)) {
                while (it.hasNext()) {
                    SourceRecord record = it.next();
                    all.add(IdempotencyKeys.withKey(record,
                            IdempotencyKeys.of("it-mysql-pg", SOURCE_TABLE, record.pk())));
                }
            }
        }
        return all;
    }

    private static TableSchema withIdempotencyKey(String table, TableSchema source) {
        List<ColumnDef> columns = new ArrayList<>(source.columns());
        columns.add(new ColumnDef(JdbcSinkWriter.IDEMPOTENCY_COLUMN, "varchar", Types.VARCHAR, 64, false));
        return new TableSchema(table, columns);
    }

    private static Connection pgConnection() {
        try {
            Properties props = new Properties();
            props.setProperty("user", ItInfra.PG_USER);
            props.setProperty("password", ItInfra.PG_PASSWORD);
            return DriverManager.getConnection(ItInfra.IT_DB_URL, props);
        } catch (Exception e) {
            throw new IllegalStateException("open PG connection failed", e);
        }
    }

    private static void cleanupMySql() {
        try (Connection my = DriverManager.getConnection(ItInfra.MYSQL_URL,
                ItInfra.MYSQL_USER, ItInfra.MYSQL_PASSWORD);
             Statement st = my.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE);
        } catch (Exception ignore) {
            // best-effort 清理
        }
    }
}
