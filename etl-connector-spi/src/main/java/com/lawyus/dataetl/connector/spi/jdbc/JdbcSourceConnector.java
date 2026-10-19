package com.lawyus.dataetl.connector.spi.jdbc;

import com.lawyus.dataetl.connector.spi.CloseableIterator;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.SourceConnector;
import com.lawyus.dataetl.connector.spi.model.ColumnDef;
import com.lawyus.dataetl.connector.spi.model.Shard;
import com.lawyus.dataetl.connector.spi.model.ShardParams;
import com.lawyus.dataetl.connector.spi.model.ShardPlan;
import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.connector.spi.model.SourceSpec;
import com.lawyus.dataetl.connector.spi.model.TableSchema;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * JDBC 参考实现（方案 §5.2）：按整型主键 {@code MIN/MAX/COUNT} 分片，逐片 {@code WHERE pk BETWEEN ? AND ?} 流式游标读取。
 *
 * <p>POSTGRES 与 MYSQL 共用本实现（差异仅在 fetchSize/流式细节），worker 侧各注册一个实例。
 * 连接由 {@link SourceSpec} 的 url/凭证即时建立，SPI 模块不耦合连接池。
 */
public class JdbcSourceConnector implements SourceConnector {

    private final ConnectorType type;

    public JdbcSourceConnector(ConnectorType type) {
        this.type = type;
    }

    @Override
    public ConnectorType type() {
        return type;
    }

    @Override
    public TableSchema readSchema(SourceSpec spec) {
        try (Connection conn = open(spec)) {
            String pk = primaryKeyOf(conn, spec.table());
            List<ColumnDef> columns = new ArrayList<>();
            DatabaseMetaData md = conn.getMetaData();
            try (ResultSet rs = md.getColumns(null, null, spec.table(), null)) {
                while (rs.next()) {
                    String name = rs.getString("COLUMN_NAME");
                    if (pk != null && pk.equalsIgnoreCase(name)) {
                        continue; // 主键列单独置顶
                    }
                    columns.add(new ColumnDef(name, rs.getString("TYPE_NAME"),
                            rs.getInt("DATA_TYPE"), rs.getInt("COLUMN_SIZE"),
                            "YES".equalsIgnoreCase(rs.getString("IS_NULLABLE"))));
                }
            }
            if (pk != null) {
                columns.add(0, pkColumn(conn, spec.table(), pk));
            }
            return new TableSchema(spec.table(), columns);
        } catch (SQLException e) {
            throw new RuntimeException("readSchema failed for " + spec.table(), e);
        }
    }

    @Override
    public ShardPlan planShards(SourceSpec spec, ShardParams params) {
        TableSchema schema = readSchema(spec);
        String pk = schema.primaryKey();
        if (pk == null) {
            throw new IllegalStateException("源表无主键，无法按主键区间分片: " + spec.table());
        }
        String sql = "SELECT MIN(" + pk + "), MAX(" + pk + "), COUNT(*) FROM " + spec.table();
        try (Connection conn = open(spec);
             Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(sql)) {
            if (!rs.next() || rs.getObject(1) == null) {
                return new ShardPlan(List.of(), 0L);   // 空表
            }
            long min = rs.getLong(1);
            long max = rs.getLong(2);
            long count = rs.getLong(3);
            // 方案 v2 D4：分片数 = 并行单元（desiredShards），chunk 只做分片内事务单元
            List<long[]> ranges = ShardRanges.splitByParallelism(
                    min, max, params.desiredShards(), params.maxShards());
            List<Shard> shards = new ArrayList<>(ranges.size());
            for (int i = 0; i < ranges.size(); i++) {
                long[] r = ranges.get(i);
                shards.add(Shard.numeric("shard-" + i, r[0], r[1]));
            }
            return new ShardPlan(shards, count);
        } catch (SQLException e) {
            throw new RuntimeException("planShards failed for " + spec.table(), e);
        }
    }

    @Override
    public CloseableIterator<SourceRecord> open(Shard shard, SourceSpec spec) {
        TableSchema schema = readSchema(spec);
        String pk = schema.primaryKey();
        // WP-02 断点续跑：resumeCursor 是“已提交的最后一行主键”，下次从 > cursor 开始（开区间）；
        // 首跑用 pkFrom（闭区间）。两者都按 pk 升序，保证不漏不重。
        Long resume = parseCursor(shard.resumeCursor());
        String sql = resume != null
                ? "SELECT * FROM " + spec.table() + " WHERE " + pk + " > ? AND " + pk + " <= ? ORDER BY " + pk
                : "SELECT * FROM " + spec.table() + " WHERE " + pk + " >= ? AND " + pk + " <= ? ORDER BY " + pk;
        try {
            // 流式游标：关 autoCommit + fetchSize（PG 要求）；MySQL 用 Integer.MIN_VALUE 逐行流式，避免全量缓冲 OOM
            Connection conn = open(spec);
            conn.setAutoCommit(false);
            java.sql.PreparedStatement ps = conn.prepareStatement(sql);
            ps.setFetchSize(type == ConnectorType.MYSQL ? Integer.MIN_VALUE : 5000);
            ps.setLong(1, resume != null ? resume : shard.pkFrom());
            ps.setLong(2, shard.pkTo());
            ResultSet rs = ps.executeQuery();
            return new ResultSetCursor(shard.shardId(), pk, rs, ps, conn);
        } catch (SQLException e) {
            throw new RuntimeException("open shard cursor failed: " + shard.shardId(), e);
        }
    }

    /** resumeCursor 解析：非数字（非 JDBC 源/空）返回 null，回落首跑语义。 */
    static Long parseCursor(String resumeCursor) {
        if (resumeCursor == null || resumeCursor.isBlank()) {
            return null;
        }
        try {
            return Long.parseLong(resumeCursor.trim());
        } catch (NumberFormatException ignore) {
            return null;
        }
    }

    // --- helpers ---

    private Connection open(SourceSpec spec) throws SQLException {
        if (spec.driverClassName() != null && !spec.driverClassName().isEmpty()) {
            try {
                Class.forName(spec.driverClassName());
            } catch (ClassNotFoundException e) {
                throw new SQLException("JDBC driver not found: " + spec.driverClassName(), e);
            }
        }
        return DriverManager.getConnection(spec.url(), spec.username(), spec.password());
    }

    private String primaryKeyOf(Connection conn, String table) throws SQLException {
        try (ResultSet rs = conn.getMetaData().getPrimaryKeys(null, null, table)) {
            if (rs.next()) {
                return rs.getString("COLUMN_NAME");
            }
        }
        return null;
    }

    private ColumnDef pkColumn(Connection conn, String table, String pk) throws SQLException {
        try (ResultSet rs = conn.getMetaData().getColumns(null, null, table, pk)) {
            if (rs.next()) {
                return new ColumnDef(pk, rs.getString("TYPE_NAME"), rs.getInt("DATA_TYPE"),
                        rs.getInt("COLUMN_SIZE"), false);
            }
        }
        return new ColumnDef(pk, "int4", java.sql.Types.INTEGER, 0, false);
    }

    /** 把 {@code ResultSet} 适配为可关闭迭代器，逐行读为 {@link SourceRecord}。 */
    private static final class ResultSetCursor implements CloseableIterator<SourceRecord> {
        private final String shardId;
        private final String pk;
        private final ResultSet rs;
        private final Statement st;
        private final Connection conn;
        private Boolean nextCached;

        ResultSetCursor(String shardId, String pk, ResultSet rs, Statement st, Connection conn) {
            this.shardId = shardId;
            this.pk = pk;
            this.rs = rs;
            this.st = st;
            this.conn = conn;
        }

        @Override
        public boolean hasNext() {
            if (nextCached == null) {
                try {
                    nextCached = rs.next();
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            }
            return nextCached;
        }

        @Override
        public SourceRecord next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            }
            try {
                ResultSetMetaData md = rs.getMetaData();
                int cols = md.getColumnCount();
                LinkedHashMap<String, Object> values = new LinkedHashMap<>();
                for (int i = 1; i <= cols; i++) {
                    values.put(md.getColumnLabel(i), rs.getObject(i));
                }
                // 本行已消费：置 null 让下一次 hasNext() 重新 rs.next() 前移游标。
                // 置 false 会把 hasNext() 永久卡在 false 上 —— 每个分片只读得到 1 行
                // （§12.1 全链路 E2E 跑真表时暴露）。
                nextCached = null;
                return new SourceRecord(shardId, rs.getObject(pk), values);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            closeQuietly(rs);
            closeQuietly(st);
            closeQuietly(conn);
        }

        private static void closeQuietly(AutoCloseable c) {
            try {
                if (c != null) {
                    c.close();
                }
            } catch (Exception ignore) {
                // best-effort
            }
        }
    }
}
