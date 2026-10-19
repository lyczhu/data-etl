package com.lawyus.dataetl.connector.spi.jdbc;

import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.SinkWriter;
import com.lawyus.dataetl.connector.spi.model.ColumnDef;
import com.lawyus.dataetl.connector.spi.model.SinkResult;
import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.connector.spi.model.SourceSpec;
import com.lawyus.dataetl.connector.spi.model.TableSchema;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.stream.Collectors;

/**
 * JDBC 参考实现：目标建表（best-effort）+ 分批 {@code executeBatch} 插入。
 *
 * <p>幂等（§4/§7.5）：Phase 2 起 {@link #ensureTargetTable} 强制 PK + {@code idempotency_key UNIQUE}，
 * 写入改 {@code INSERT ... ON CONFLICT DO NOTHING} 并回填 {@link SinkResult#rowsDeduped}；
 * 本骨架先做普通批插，整批失败时逐行降级、失败行计 {@code rowsDropped}（其余提交）。
 */
public class JdbcSinkWriter implements SinkWriter {

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
    public void ensureTargetTable(TableSchema schema, SourceSpec target) {
        String ddl = buildCreateTable(schema);
        try (Connection conn = connections.getConnection();
             Statement st = conn.createStatement()) {
            st.execute(ddl);
        } catch (SQLException e) {
            throw new RuntimeException("ensureTargetTable failed: " + schema.table(), e);
        }
    }

    @Override
    public SinkResult writeBatch(TableSchema schema, List<SourceRecord> batch) {
        if (batch == null || batch.isEmpty()) {
            return SinkResult.zero();
        }
        List<String> cols = schema.columns().stream().map(ColumnDef::name).collect(Collectors.toList());
        String sql = buildInsert(schema.table(), cols);
        try (Connection conn = connections.getConnection()) {
            conn.setAutoCommit(false);
            try (PreparedStatement ps = conn.prepareStatement(sql)) {
                for (SourceRecord rec : batch) {
                    for (int i = 0; i < cols.size(); i++) {
                        ps.setObject(i + 1, rec.columns().get(cols.get(i)));
                    }
                    ps.addBatch();
                }
                int[] r = ps.executeBatch();
                conn.commit();
                return new SinkResult(countAffected(r), 0, 0);
            } catch (SQLException batchFail) {
                conn.rollback();
                return writeRowByRow(conn, sql, cols, batch);   // 降级：逐行，坏行进 dropped
            }
        } catch (SQLException e) {
            throw new RuntimeException("writeBatch failed: " + schema.table(), e);
        }
    }

    private SinkResult writeRowByRow(Connection conn, String sql, List<String> cols, List<SourceRecord> batch)
            throws SQLException {
        long out = 0;
        long dropped = 0;
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            for (SourceRecord rec : batch) {
                for (int i = 0; i < cols.size(); i++) {
                    ps.setObject(i + 1, rec.columns().get(cols.get(i)));
                }
                try {
                    out += Math.max(0, ps.executeUpdate());
                } catch (SQLException rowFail) {
                    dropped++;   // 该 FATAL 行后续应送 DLT
                }
            }
        }
        return new SinkResult(out, dropped, 0);
    }

    private static long countAffected(int[] r) {
        long total = 0;
        for (int n : r) {
            if (n >= 0) {
                total += n;
            } else if (n == Statement.SUCCESS_NO_INFO) {
                total += 1;
            }
        }
        return total;
    }

    String buildInsert(String table, List<String> cols) {
        String placeholders = cols.stream().map(c -> "?").collect(Collectors.joining(", "));
        return "INSERT INTO " + table + " (" + String.join(", ", cols) + ") VALUES (" + placeholders + ")";
    }

    /** 可见以便单测；best-effort 建表（沿用源列类型，Phase 2 再补 PK/唯一索引）。 */
    String buildCreateTable(TableSchema schema) {
        String cols = schema.columns().stream()
                .map(c -> c.name() + " " + c.typeName())
                .collect(Collectors.joining(", "));
        return "CREATE TABLE IF NOT EXISTS " + schema.table() + " (" + cols + ")";
    }
}
