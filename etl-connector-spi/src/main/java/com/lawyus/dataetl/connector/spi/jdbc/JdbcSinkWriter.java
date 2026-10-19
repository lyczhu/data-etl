package com.lawyus.dataetl.connector.spi.jdbc;

import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.SinkWriter;
import com.lawyus.dataetl.connector.spi.model.ColumnDef;
import com.lawyus.dataetl.connector.spi.model.FailedRow;
import com.lawyus.dataetl.connector.spi.model.SinkResult;
import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.connector.spi.model.SourceSpec;
import com.lawyus.dataetl.connector.spi.model.TableSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * JDBC 参考实现：目标建表 + 幂等批写（方案 v2 WP-03/WP-04/WP-07）。
 *
 * <ul>
 *   <li>目标表强制 {@code idempotency_key VARCHAR(64) PRIMARY KEY}（若 schema 已带则不重复追加）；</li>
 *   <li>PostgreSQL 用 {@code ON CONFLICT (idempotency_key) DO NOTHING}，MySQL 用
 *       {@code ON DUPLICATE KEY UPDATE idempotency_key = idempotency_key}；</li>
 *   <li>整批失败时回滚到 savepoint 并逐行降级，失败行产出 {@link FailedRow}（原文供 DLT）；</li>
 *   <li>空间列写入用 {@code ST_GeomFromText(?,4326)}，数值列不再做默认精度截断（WP-07）。</li>
 * </ul>
 *
 * <p>事务：{@link #writeBatch(Connection, TableSchema, List)} 不 commit/rollback 整个事务，
 * 供 worker 把“目标写入 + checkpoint”放进同一本地事务（WP-02）；{@link #writeBatch(TableSchema, List)}
 * 是独立连接入口，供非事务场景使用。
 */
public class JdbcSinkWriter implements SinkWriter {

    public static final String IDEMPOTENCY_COLUMN = "idempotency_key";
    private static final int RAW_PAYLOAD_LIMIT = 64 * 1024;

    private final ConnectorType type;
    private final ConnectionProvider connections;

    public JdbcSinkWriter(ConnectorType type, ConnectionProvider connections) {
        this.type = type;
        this.connections = connections;
    }

    @Override
    public ConnectorType type() {
        return type;
    }

    @Override
    public boolean supportsExternalTransaction() {
        return true;
    }

    @Override
    public void ensureTargetTable(TableSchema schema, SourceSpec target) {
        String ddl = buildCreateTable(schema);
        try (Connection conn = connections.getConnection();
             Statement st = conn.createStatement()) {
            st.execute(ddl);
        } catch (SQLException e) {
            // 并发分片同时 CREATE TABLE IF NOT EXISTS：PG 仍可能因 pg_type 唯一冲突报 23505/42P07，按幂等处理
            if ("23505".equals(e.getSQLState()) || "42P07".equals(e.getSQLState())) {
                return;
            }
            throw new RuntimeException("ensureTargetTable failed: " + schema.table(), e);
        }
    }

    @Override
    public SinkResult writeBatch(TableSchema schema, List<SourceRecord> batch) {
        if (batch == null || batch.isEmpty()) {
            return SinkResult.zero();
        }
        try (Connection conn = connections.getConnection()) {
            conn.setAutoCommit(false);
            try {
                SinkResult result = writeBatch(conn, schema, batch);
                conn.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                conn.rollback();
                // 连接级失败：单行重试一次，显式 commit（P0-4：逐行降级不再依赖 close 隐式回滚）
                return writeRowByRowStandalone(schema, batch, e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("writeBatch failed: " + schema.table(), e);
        }
    }

    @Override
    public SinkResult writeBatch(Connection conn, TableSchema schema, List<SourceRecord> batch) {
        if (batch == null || batch.isEmpty()) {
            return SinkResult.zero();
        }
        List<String> cols = schema.columns().stream().map(ColumnDef::name).collect(Collectors.toList());
        String sql = buildUpsert(schema, cols);
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            Savepoint batchPoint = conn.setSavepoint();
            try {
                for (SourceRecord rec : batch) {
                    bind(ps, schema, cols, rec);
                    ps.addBatch();
                }
                int[] counts = ps.executeBatch();
                return fromCounts(counts);
            } catch (SQLException batchFailure) {
                conn.rollback(batchPoint);
                return writeRowByRow(conn, sql, schema, cols, batch);
            }
        } catch (SQLException e) {
            throw new RuntimeException("writeBatch failed: " + schema.table(), e);
        }
    }

    /** 逐行降级：每行一个 savepoint，坏行回滚到 savepoint 并记入 failures，不污染好行。 */
    private SinkResult writeRowByRow(Connection conn, String sql, TableSchema schema,
                                     List<String> cols, List<SourceRecord> batch) throws SQLException {
        long out = 0;
        long deduped = 0;
        List<FailedRow> failures = new ArrayList<>();
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (SourceRecord rec : batch) {
                Savepoint rowPoint = conn.setSavepoint();
                try {
                    bind(ps, schema, cols, rec);
                    int n = ps.executeUpdate();
                    if (n == 0) {
                        deduped++;
                    } else {
                        out += n;
                    }
                } catch (SQLException | RuntimeException rowFail) {
                    conn.rollback(rowPoint);
                    failures.add(new FailedRow(classify(rowFail), abbreviate(rowFail.getMessage()),
                            rawPayload(rec), String.valueOf(rec.pk())));
                }
            }
        }
        return new SinkResult(out, 0, failures.size(), deduped, failures);
    }

    /** 独立事务的逐行降级（连接级失败兜底）：每行成功后累积，最后显式 commit。 */
    private SinkResult writeRowByRowStandalone(TableSchema schema, List<SourceRecord> batch, Exception cause) {
        List<String> cols = schema.columns().stream().map(ColumnDef::name).collect(Collectors.toList());
        String sql = buildUpsert(schema, cols);
        long out = 0;
        long deduped = 0;
        List<FailedRow> failures = new ArrayList<>();
        try (Connection conn = connections.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (SourceRecord rec : batch) {
                    Savepoint rowPoint = conn.setSavepoint();
                    try {
                        bind(ps, schema, cols, rec);
                        int n = ps.executeUpdate();
                        if (n == 0) {
                            deduped++;
                        } else {
                            out += n;
                        }
                    } catch (SQLException | RuntimeException rowFail) {
                        conn.rollback(rowPoint);
                        failures.add(new FailedRow(classify(rowFail), abbreviate(rowFail.getMessage()),
                                rawPayload(rec), String.valueOf(rec.pk())));
                    }
                }
                conn.commit();   // 显式提交逐行成功的数据（P0-4）
                if (out == 0 && failures.isEmpty()) {
                    failures.add(new FailedRow(classify(cause), abbreviate(cause.getMessage()), null));
                }
                return new SinkResult(out, 0, failures.size(), deduped, failures);
            } catch (SQLException e) {
                conn.rollback();
                throw new RuntimeException("row-by-row fallback failed: " + schema.table(), e);
            }
        } catch (SQLException e) {
            throw new RuntimeException("row-by-row fallback connection failed: " + schema.table(), e);
        }
    }

    private void bind(PreparedStatement ps, TableSchema schema, List<String> cols, SourceRecord rec)
            throws SQLException {
        for (int i = 0; i < cols.size(); i++) {
            String col = cols.get(i);
            Object value = rec.columns().get(col);
            ColumnDef def = column(schema, col);
            if (def != null && TypeMapping.isGeometry(def.typeName()) && value instanceof String wkt) {
                // 目标列由 buildCreateTable 建成 geometry(Geometry,4326)，写入时显式带 SRID
                if (type == ConnectorType.MYSQL) {
                    ps.setString(i + 1, wkt);
                } else {
                    ps.setString(i + 1, withSrid(wkt));
                }
            } else {
                ps.setObject(i + 1, value);
            }
        }
    }

    /** 把 {@code POINT (1 2)} 规范为 {@code SRID=4326;POINT (1 2)}（PostGIS EWKT）。 */
    static String withSrid(String wkt) {
        String t = wkt.trim();
        return t.regionMatches(true, 0, "SRID=", 0, 5) ? t : "SRID=4326;" + t;
    }

    private static ColumnDef column(TableSchema schema, String name) {
        for (ColumnDef c : schema.columns()) {
            if (c.name().equals(name)) {
                return c;
            }
        }
        return null;
    }

    private static SinkResult fromCounts(int[] counts) {
        long out = 0;
        long deduped = 0;
        for (int n : counts) {
            if (n > 0) {
                out += n;
            } else if (n == 0) {
                deduped++;
            } else if (n == Statement.SUCCESS_NO_INFO) {
                out += 1;
            }
        }
        return new SinkResult(out, 0, 0, deduped, List.of());
    }

    private static String classify(Throwable e) {
        if (e instanceof SQLException se) {
            String state = se.getSQLState();
            if (state != null && (state.startsWith("08") || state.startsWith("40")
                    || "55P03".equals(state) || "40001".equals(state))) {
                return "TRANSIENT";
            }
            if (state != null && state.startsWith("23")) {
                return "FATAL";
            }
        }
        return "PERSISTENT";
    }

    private static String abbreviate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 2000 ? s : s.substring(0, 2000);
    }

    private static String rawPayload(SourceRecord rec) {
        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        boolean first = true;
        for (var e : rec.columns().entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(escape(e.getKey())).append("\":\"")
                    .append(escape(String.valueOf(e.getValue()))).append('"');
        }
        sb.append('}');
        return sb.length() <= RAW_PAYLOAD_LIMIT ? sb.toString() : sb.substring(0, RAW_PAYLOAD_LIMIT);
    }

    private static String escape(String s) {
        return s == null ? "" : s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    /** 普通 INSERT（无幂等键时兜底）；保留可见性供 SQL 单测。 */
    String buildInsert(String table, List<String> cols) {
        String placeholders = cols.stream().map(c -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + table + " (" + String.join(", ", cols) + ") VALUES (" + placeholders + ")";
    }

    /** 带幂等/空间语义的 upsert。 */
    String buildUpsert(TableSchema schema, List<String> cols) {
        String placeholders = cols.stream()
                .map(c -> valueExpression(column(schema, c)))
                .collect(Collectors.joining(", "));
        String sql = "INSERT INTO " + schema.table() + " (" + String.join(", ", cols) + ") VALUES (" + placeholders + ")";
        if (!cols.contains(IDEMPOTENCY_COLUMN)) {
            return sql;
        }
        if (type == ConnectorType.MYSQL) {
            return sql + " ON DUPLICATE KEY UPDATE " + IDEMPOTENCY_COLUMN + " = " + IDEMPOTENCY_COLUMN;
        }
        return sql + " ON CONFLICT (" + IDEMPOTENCY_COLUMN + ") DO NOTHING";
    }

    private String valueExpression(ColumnDef def) {
        if (def != null && TypeMapping.isGeometry(def.typeName()) && type != ConnectorType.MYSQL) {
            return "ST_GeomFromEWKT(?)";
        }
        if (def != null && TypeMapping.isGeometry(def.typeName())) {
            return "ST_GeomFromText(?)";
        }
        return "?";
    }

    /** 可见以便单测；强制 idempotency_key 主键（已存在则不重复追加），类型按目标库映射。 */
    String buildCreateTable(TableSchema schema) {
        return buildCreateTable(type, schema);
    }

    /**
     * 公共 DDL 生成（WP-13：orchestrator V-3 在提交时一次性建表，worker 不再每分片建表）。
     */
    public static String buildCreateTable(ConnectorType type, TableSchema schema) {
        List<String> parts = new ArrayList<>();
        for (ColumnDef c : schema.columns()) {
            if (IDEMPOTENCY_COLUMN.equalsIgnoreCase(c.name())) {
                continue;
            }
            String mapped = type == ConnectorType.MYSQL
                    ? TypeMapping.toMysql(c.typeName(), c.size())
                    : TypeMapping.toPostgres(c.typeName(), c.size());
            parts.add(c.name() + " " + mapped);
        }
        // 无论源表是否已带同名列，目标表都以 idempotency_key 作为主键（WP-03）
        parts.add(IDEMPOTENCY_COLUMN + " VARCHAR(64) PRIMARY KEY");
        return "CREATE TABLE IF NOT EXISTS " + schema.table() + " (" + String.join(", ", parts) + ")";
    }
}
