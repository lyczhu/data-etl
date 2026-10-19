package com.lawyus.dataetl.worker.checkpoint;

import com.lawyus.dataetl.connector.spi.model.FailedRow;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

/**
 * {@code etl_task_dlt} 写入器（WP-04/WP-05）：坏行原文与错误分类落表，供 {@code /failures} 查询
 * 与“未解决 DLT 阻止 SUCCEEDED”守卫使用。
 */
@Component
public class DltWriter {

    private static final int RAW_LIMIT = 64 * 1024;

    private static final String INSERT =
            "INSERT INTO etl_task_dlt (task_id, run_id, shard_id, error_class, error_message, raw_payload) "
                    + "VALUES (?, ?, ?, ?, ?, ?)";

    private final JdbcTemplate jdbcTemplate;

    public DltWriter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** 参与调用方事务（JdbcTemplate 自动加入 TransactionTemplate 事务）。 */
    public void insertAll(String taskId, String runId, String shardId, List<FailedRow> failures) {
        if (failures == null || failures.isEmpty()) {
            return;
        }
        jdbcTemplate.batchUpdate(INSERT, failures.stream()
                .map(f -> new Object[]{taskId, runId, shardId,
                        f.errorClass(), truncate(f.errorMessage()), truncate(f.rawPayload())})
                .toList());
    }

    /** 显式连接入口（与 sink/checkpoint 同事务）。 */
    public void insertAll(Connection conn, String taskId, String runId, String shardId,
                          List<FailedRow> failures) throws SQLException {
        if (failures == null || failures.isEmpty()) {
            return;
        }
        try (PreparedStatement ps = conn.prepareStatement(INSERT)) {
            for (FailedRow f : failures) {
                ps.setString(1, taskId);
                ps.setString(2, runId);
                ps.setString(3, shardId);
                ps.setString(4, f.errorClass());
                ps.setString(5, truncate(f.errorMessage()));
                ps.setString(6, truncate(f.rawPayload()));
                ps.addBatch();
            }
            ps.executeBatch();
        }
    }

    /** 分片级错误（无单行原文）也落一条，保证 failures 可查。 */
    public void insertShardFailure(String taskId, String runId, String shardId,
                                   String errorClass, String errorMessage) {
        jdbcTemplate.update(INSERT, taskId, runId, shardId, errorClass, truncate(errorMessage), null);
    }

    private static String truncate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= RAW_LIMIT ? s : s.substring(0, RAW_LIMIT);
    }
}
