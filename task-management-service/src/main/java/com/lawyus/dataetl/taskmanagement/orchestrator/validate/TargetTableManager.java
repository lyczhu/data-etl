package com.lawyus.dataetl.taskmanagement.orchestrator.validate;

import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.jdbc.JdbcSinkWriter;
import com.lawyus.dataetl.connector.spi.model.TableSchema;
import com.lawyus.dataetl.taskmanagement.client.DataSourceServiceClient;
import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Pattern;

/**
 * 目标表治理（方案 v2 WP-13 / Gate 2）：
 *
 * <ul>
 *   <li>V-3 在提交时一次性建表/校验（目标库 advisory lock 串行化并发提交）；</li>
 *   <li>worker 不再每个分片执行 {@code CREATE TABLE IF NOT EXISTS}，避免并发建表冲突与 DDL 放大；</li>
 *   <li>存量目标表缺 {@code idempotency_key} 时补列并建唯一索引；若已有重复键则失败，
 *       提示先清洗（不在线自动删数据）。</li>
 * </ul>
 *
 * <p>目标库：{@link EtlTask#getTargetDataSourceId()} 非空时由 data-source-service 解析；
 * 为空时按同库部署使用 orchestrator 自己的 DataSource。Phase 1B 只对 PostgreSQL 做 DDL 治理，
 * MySQL 目标库走同一接口但索引探测语句不同。
 */
@Service
public class TargetTableManager {

    private static final Logger log = LoggerFactory.getLogger(TargetTableManager.class);
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final String IDEMPOTENCY_COLUMN = JdbcSinkWriter.IDEMPOTENCY_COLUMN;

    private final DataSourceServiceClient dataSourceServiceClient;
    private final javax.sql.DataSource orchestratorDataSource;

    public TargetTableManager(DataSourceServiceClient dataSourceServiceClient,
                              javax.sql.DataSource orchestratorDataSource) {
        this.dataSourceServiceClient = dataSourceServiceClient;
        this.orchestratorDataSource = orchestratorDataSource;
    }

    /** V-3：确保目标表可写（表存在且带 idempotency_key 唯一键）。失败抛 {@link IllegalStateException}。 */
    public void ensure(EtlTask task, TableSchema sourceSchema) {
        if (sourceSchema == null || sourceSchema.columns() == null || sourceSchema.columns().isEmpty()) {
            throw new IllegalStateException("source schema is empty");
        }
        String table = requireSafeIdentifier(task.getTargetTableName());
        TargetConnection target = openTarget(task);
        try (Connection conn = target.connection()) {
            conn.setAutoCommit(false);
            try {
                advisoryLock(conn, target.type(), table);
                if (!tableExists(conn, target.type(), table)) {
                    String ddl = JdbcSinkWriter.buildCreateTable(target.type(),
                            new TableSchema(table, sourceSchema.columns()));
                    execute(conn, ddl);
                }
                ensureIdempotencyKey(conn, target.type(), table);
                conn.commit();
                log.info("target table ready: table={} type={} targetDataSource={}",
                        table, target.type(), task.getTargetDataSourceId());
            } catch (SQLException | RuntimeException e) {
                rollbackQuietly(conn);
                throw new IllegalStateException("target table not writable: " + table
                        + " (" + e.getMessage() + ")", e);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("target table not writable: " + table, e);
        }
    }

    private TargetConnection openTarget(EtlTask task) {
        try {
            if (task.getTargetDataSourceId() == null) {
                return new TargetConnection(ConnectorType.POSTGRES, orchestratorDataSource.getConnection());
            }
            com.lawyus.dataetl.taskmanagement.client.DataSource ds =
                    dataSourceServiceClient.getDataSourceById(task.getTargetDataSourceId());
            if (ds == null) {
                throw new IllegalStateException("target datasource unreachable: " + task.getTargetDataSourceId());
            }
            ConnectorType type = ShardPlanner.mapType(ds.getType());
            return new TargetConnection(type,
                    DriverManager.getConnection(ds.getUrl(), ds.getUsername(), ds.getPassword()));
        } catch (SQLException e) {
            throw new IllegalStateException("target datasource connection failed: "
                    + task.getTargetDataSourceId(), e);
        }
    }

    /** 并发提交同一目标表时用 advisory lock 串行化；PG 事务级锁在 commit 时释放。 */
    private void advisoryLock(Connection conn, ConnectorType type, String table) throws SQLException {
        String key = "etl:target:" + table;
        String sql = type == ConnectorType.MYSQL
                ? "SELECT GET_LOCK(?, 30)"
                : "SELECT pg_advisory_xact_lock(hashtext(?))";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, key);
            ps.execute();
        }
    }

    private boolean tableExists(Connection conn, ConnectorType type, String table) throws SQLException {
        String sql = type == ConnectorType.MYSQL
                ? "SELECT 1 FROM information_schema.tables "
                + "WHERE table_schema = DATABASE() AND table_name = ?"
                : "SELECT to_regclass(?)";
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() && rs.getString(1) != null;
            }
        }
    }

    private void ensureIdempotencyKey(Connection conn, ConnectorType type, String table) throws SQLException {
        String quotedTable = quote(type, table);
        boolean hasColumn = exists(conn, type == ConnectorType.MYSQL
                ? "SELECT 1 FROM information_schema.columns "
                + "WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ?"
                : "SELECT 1 FROM information_schema.columns "
                + "WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?",
                table, IDEMPOTENCY_COLUMN);
        if (!hasColumn) {
            execute(conn, "ALTER TABLE " + quotedTable + " ADD COLUMN " + IDEMPOTENCY_COLUMN + " VARCHAR(64)");
        }

        boolean hasUnique = exists(conn, type == ConnectorType.MYSQL
                ? "SELECT 1 FROM information_schema.statistics "
                + "WHERE table_schema = DATABASE() AND table_name = ? AND column_name = ? AND non_unique = 0"
                : "SELECT 1 FROM pg_indexes "
                + "WHERE schemaname = current_schema() AND tablename = ? "
                + "AND indexdef ILIKE ? AND indexdef ILIKE '%UNIQUE%'",
                table, type == ConnectorType.MYSQL ? IDEMPOTENCY_COLUMN : "%" + IDEMPOTENCY_COLUMN + "%");
        if (!hasUnique) {
            String index = indexName(table);
            execute(conn, "CREATE UNIQUE INDEX " + index + " ON " + quotedTable
                    + " (" + IDEMPOTENCY_COLUMN + ")");
        }
    }

    private boolean exists(Connection conn, String sql, String table, String columnOrPattern) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, table);
            ps.setString(2, columnOrPattern);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }
    }

    private static void execute(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement()) {
            st.execute(sql);
        }
    }

    private static void rollbackQuietly(Connection conn) {
        try {
            conn.rollback();
        } catch (SQLException ignored) {
            // 原始异常更重要
        }
    }

    private static String quote(ConnectorType type, String identifier) {
        return type == ConnectorType.MYSQL ? "`" + identifier + "`" : "\"" + identifier + "\"";
    }

    private static String indexName(String table) {
        String name = "ux_" + table + "_idem";
        if (name.length() <= 63) {
            return name;
        }
        return "ux_" + Integer.toHexString(table.hashCode()) + "_idem";
    }

    private static String requireSafeIdentifier(String name) {
        if (name == null || !SAFE_IDENTIFIER.matcher(name).matches() || name.length() > 63) {
            throw new IllegalArgumentException("unsafe target table name: " + name);
        }
        return name;
    }

    private record TargetConnection(ConnectorType type, Connection connection) {
    }
}
