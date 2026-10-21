package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.connector.spi.CloseableIterator;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.jdbc.JdbcSinkWriter;
import com.lawyus.dataetl.connector.spi.jdbc.JdbcSourceConnector;
import com.lawyus.dataetl.connector.spi.jdbc.TypeMapping;
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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2A：PostGIS 真库回归（Gate 1 遗留项）。
 *
 * <p>覆盖 Point / MultiPolygon / GeometryCollection 三种几何，验证 SPI 读→写后
 * 目标列仍为 {@code geometry(Geometry,4326)} 且 SRID 不变。
 */
@Tag("it")
class PostgisSinkIT {

    private static final String SOURCE_TABLE = "it_postgis_source";
    private static final String TARGET_TABLE = "it_postgis_target";
    private static final int ROWS = 3;

    @BeforeAll
    static void infra() {
        ItInfra.assumeInfrastructureUpAndBootstrap();
    }

    @Test
    @DisplayName("PostGIS 三种几何 → 目标表：行数/SRID/WKT 一致，重放全部幂等")
    void postgisGeometry_roundTripsAndKeepsSrid() throws Exception {
        try (Connection pg = pgConnection(); Statement st = pg.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + TARGET_TABLE);
            st.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE + " CASCADE");
            st.execute("CREATE TABLE " + SOURCE_TABLE + " ("
                    + "id BIGINT PRIMARY KEY, name VARCHAR(64), geom geometry(Geometry,4326))");
            st.execute("INSERT INTO " + SOURCE_TABLE + " (id, name, geom) VALUES "
                    + "(1, 'point', ST_GeomFromText('POINT(120.1 30.2)', 4326)),"
                    + "(2, 'multi', ST_GeomFromText('MULTIPOLYGON(((0 0,0 1,1 1,1 0,0 0)))', 4326)),"
                    + "(3, 'collection', ST_GeomFromText('GEOMETRYCOLLECTION(POINT(1 2),LINESTRING(0 0,1 1))', 4326))");
        }

        try {
            SourceSpec sourceSpec = new SourceSpec(ConnectorType.POSTGRES, ItInfra.IT_DB_URL,
                    ItInfra.PG_USER, ItInfra.PG_PASSWORD, "org.postgresql.Driver", SOURCE_TABLE, 30);
            JdbcSourceConnector source = new JdbcSourceConnector(ConnectorType.POSTGRES);
            JdbcSinkWriter sink = new JdbcSinkWriter(ConnectorType.POSTGRES, PostgisSinkIT::pgConnection);

            TableSchema sourceSchema = source.readSchema(sourceSpec);
            ColumnDef geom = sourceSchema.columns().stream()
                    .filter(c -> "geom".equalsIgnoreCase(c.name()))
                    .findFirst().orElseThrow();
            assertThat(TypeMapping.isGeometry(geom.typeName())).isTrue();

            TableSchema targetSchema = withIdempotencyKey(TARGET_TABLE, sourceSchema);
            sink.ensureTargetTable(targetSchema, new SourceSpec(ConnectorType.POSTGRES,
                    ItInfra.IT_DB_URL, ItInfra.PG_USER, ItInfra.PG_PASSWORD,
                    "org.postgresql.Driver", TARGET_TABLE));

            ShardPlan plan = source.planShards(sourceSpec, new ShardParams(8, 100, 2));
            List<SourceRecord> all = readAll(source, sourceSpec, plan);
            assertThat(all).hasSize(ROWS);

            SinkResult first = sink.writeBatch(targetSchema, all);
            assertThat(first.rowsOut()).isEqualTo(ROWS);
            assertThat(first.rowsFailed()).isZero();

            SinkResult replay = sink.writeBatch(targetSchema, all);
            assertThat(replay.rowsDeduped()).isEqualTo(ROWS);

            try (Connection pg = pgConnection(); Statement st = pg.createStatement()) {
                try (ResultSet rs = st.executeQuery("SELECT COUNT(*), "
                        + "COUNT(*) FILTER (WHERE ST_SRID(geom) = 4326) FROM " + TARGET_TABLE)) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getLong(1)).isEqualTo(ROWS);
                    assertThat(rs.getLong(2)).isEqualTo(ROWS);
                }
                try (ResultSet rs = st.executeQuery("SELECT ST_AsText(geom) FROM " + TARGET_TABLE
                        + " WHERE id = 1")) {
                    assertThat(rs.next()).isTrue();
                    assertThat(rs.getString(1)).isEqualTo("POINT(120.1 30.2)");
                }
                try (ResultSet rs = st.executeQuery("SELECT GeometryType(geom) FROM " + TARGET_TABLE
                        + " ORDER BY id")) {
                    List<String> types = new ArrayList<>();
                    while (rs.next()) {
                        types.add(rs.getString(1));
                    }
                    assertThat(types).containsExactly("POINT", "MULTIPOLYGON", "GEOMETRYCOLLECTION");
                }
            }
        } finally {
            try (Connection pg = pgConnection(); Statement st = pg.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + TARGET_TABLE);
                st.execute("DROP TABLE IF EXISTS " + SOURCE_TABLE + " CASCADE");
            }
        }
    }

    private static List<SourceRecord> readAll(JdbcSourceConnector source, SourceSpec spec, ShardPlan plan) {
        List<SourceRecord> all = new ArrayList<>(ROWS);
        for (Shard shard : plan.shards()) {
            try (CloseableIterator<SourceRecord> it = source.open(shard, spec)) {
                while (it.hasNext()) {
                    SourceRecord record = it.next();
                    all.add(IdempotencyKeys.withKey(record,
                            IdempotencyKeys.of("it-postgis", SOURCE_TABLE, record.pk())));
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
}
