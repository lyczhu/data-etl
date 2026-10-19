package com.lawyus.dataetl.worker.checkpoint;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * checkpoint 直写器（worker 是 {@code etl_task_checkpoint} 的唯一写者，写权矩阵 §4.1）。
 *
 * <p>WP-02/WP-11 的四条约束：
 * <ul>
 *   <li><b>chunk 级游标</b>：每个 chunk 写 {@code last_cursor/last_pk}，重试从游标之后续读；</li>
 *   <li><b>attempt fencing</b>：写入带 {@code shard_attempt}，旧 attempt 的迟到写入被忽略；</li>
 *   <li><b>lease fencing</b>：写入带 {@code lease_owner}，lease 被新 worker 接管后旧 worker 的迟到写入被忽略；</li>
 *   <li><b>计数单调</b>：{@code rows_*} 用 {@code GREATEST} 保序，跨 attempt 累计不回退。</li>
 * </ul>
 *
 * <p>同库提交（WP-02）：worker 在同一个 {@code TransactionTemplate} 内调用
 * {@link #upsert(Connection, String, String, String, String, String, String, int, long, long, long, long, long, String, String, String, String)}
 * 与 {@code JdbcSinkWriter.writeBatch(Connection,...)}，由调用方统一 commit/rollback。
 */
@Component
public class CheckpointWriter {

    private static final String UPSERT =
            "INSERT INTO etl_task_checkpoint "
            + "(task_id, run_id, shard_id, status, shard_attempt, last_cursor, last_pk, "
            + " rows_in, rows_out, rows_filtered, rows_failed, rows_dropped, rows_deduped, "
            + " lease_owner, error_class, error_detail, updated_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()) "
            + "ON CONFLICT (task_id, run_id, shard_id) DO UPDATE SET "
            // fencing：旧 attempt 的迟到写入整体忽略
            + "  status = CASE "
            + "    WHEN EXCLUDED.shard_attempt < etl_task_checkpoint.shard_attempt THEN etl_task_checkpoint.status "
            // 终态保护：已 SUCCEEDED/FAILED 的分片不允许被同/旧 attempt 回退到 RUNNING/ASSIGNED
            + "    WHEN etl_task_checkpoint.status IN ('SUCCEEDED','FAILED') "
            + "         AND EXCLUDED.status IN ('ASSIGNED','RUNNING') THEN etl_task_checkpoint.status "
            + "    ELSE EXCLUDED.status END, "
            + "  shard_attempt = GREATEST(etl_task_checkpoint.shard_attempt, EXCLUDED.shard_attempt), "
            + "  last_cursor = CASE WHEN EXCLUDED.shard_attempt < etl_task_checkpoint.shard_attempt "
            + "    THEN etl_task_checkpoint.last_cursor ELSE COALESCE(EXCLUDED.last_cursor, etl_task_checkpoint.last_cursor) END, "
            + "  last_pk = CASE WHEN EXCLUDED.shard_attempt < etl_task_checkpoint.shard_attempt "
            + "    THEN etl_task_checkpoint.last_pk ELSE COALESCE(EXCLUDED.last_pk, etl_task_checkpoint.last_pk) END, "
            + "  rows_in = GREATEST(etl_task_checkpoint.rows_in, EXCLUDED.rows_in), "
            + "  rows_out = GREATEST(etl_task_checkpoint.rows_out, EXCLUDED.rows_out), "
            + "  rows_filtered = GREATEST(etl_task_checkpoint.rows_filtered, EXCLUDED.rows_filtered), "
            + "  rows_failed = GREATEST(etl_task_checkpoint.rows_failed, EXCLUDED.rows_failed), "
            + "  rows_dropped = GREATEST(etl_task_checkpoint.rows_dropped, EXCLUDED.rows_dropped), "
            + "  rows_deduped = GREATEST(etl_task_checkpoint.rows_deduped, EXCLUDED.rows_deduped), "
            + "  lease_owner = COALESCE(EXCLUDED.lease_owner, etl_task_checkpoint.lease_owner), "
            + "  error_class = CASE WHEN EXCLUDED.shard_attempt < etl_task_checkpoint.shard_attempt "
            + "    THEN etl_task_checkpoint.error_class ELSE EXCLUDED.error_class END, "
            + "  error_detail = CASE WHEN EXCLUDED.shard_attempt < etl_task_checkpoint.shard_attempt "
            + "    THEN etl_task_checkpoint.error_detail ELSE EXCLUDED.error_detail END, "
            + "  updated_at = now() "
            // lease fencing：同 attempt 下只有当前 owner（或仍无 owner 的旧行）可写；
            // 旧 owner 在新 worker 接管 lease 后的迟到写入会被这条 WHERE 拒绝。
            + "WHERE EXCLUDED.shard_attempt > etl_task_checkpoint.shard_attempt "
            + "   OR (EXCLUDED.shard_attempt = etl_task_checkpoint.shard_attempt "
            + "       AND (EXCLUDED.lease_owner IS NULL OR etl_task_checkpoint.lease_owner IS NULL "
            + "            OR etl_task_checkpoint.lease_owner = EXCLUDED.lease_owner))";

    private static final String ACQUIRE_LEASE =
            "UPDATE etl_task_checkpoint "
            + "SET lease_owner = ?, lease_until = now() + (? * interval '1 second'), "
            + "    shard_attempt = GREATEST(shard_attempt, ?), updated_at = now() "
            + "WHERE task_id = ? AND run_id = ? AND shard_id = ? "
            + "  AND shard_attempt <= ? "
            + "  AND (lease_until IS NULL OR lease_until < now() OR lease_owner = ?)";

    private static final String RENEW_LEASE =
            "UPDATE etl_task_checkpoint SET lease_until = ?, updated_at = now() "
            + "WHERE task_id = ? AND run_id = ? AND shard_id = ? AND shard_attempt = ? AND lease_owner = ?";

    private final JdbcTemplate jdbcTemplate;

    public CheckpointWriter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /**
     * WP-11：分片执行前获取 lease。返回 1 表示成功；0 表示另一 worker 持有有效 lease，
     * 或当前 attempt 比库内旧（迟到 worker）。lease 过期后可被新 worker 直接接管。
     */
    public int tryAcquireLease(String taskId, String runId, String shardId, int shardAttempt,
                               String leaseOwner, long leaseSeconds) {
        double seconds = Math.max(1L, leaseSeconds);
        return jdbcTemplate.update(ACQUIRE_LEASE, leaseOwner, seconds, shardAttempt,
                taskId, runId, shardId, shardAttempt, leaseOwner);
    }

    /** JdbcTemplate 入口：参与调用方事务（若在 TransactionTemplate 内）。 */
    public int upsert(String taskId, String runId, String shardId, String status, String lastCursor,
                      long rowsIn, long rowsOut, long rowsDropped, long rowsDeduped,
                      String errorClass, String errorDetail) {
        // 兼容旧签名：rowsDropped 视为业务过滤（旧口径），attempt=0
        return upsert(taskId, runId, shardId, status, lastCursor, null, 0,
                rowsIn, rowsOut, rowsDropped, 0, rowsDropped, rowsDeduped, null, errorClass, errorDetail);
    }

    /** JdbcTemplate 入口：完整字段（参与调用方事务），leaseOwner 可为 null（旧调用/测试）。 */
    public int upsert(String taskId, String runId, String shardId, String status, String lastCursor,
                      String lastPk, int shardAttempt,
                      long rowsIn, long rowsOut, long rowsFiltered, long rowsFailed, long rowsDropped,
                      long rowsDeduped, String errorClass, String errorDetail) {
        return upsert(taskId, runId, shardId, status, lastCursor, lastPk, shardAttempt,
                rowsIn, rowsOut, rowsFiltered, rowsFailed, rowsDropped, rowsDeduped,
                null, errorClass, errorDetail);
    }

    /** JdbcTemplate 入口：带 leaseOwner 的完整字段（WP-11 生产路径）。 */
    public int upsert(String taskId, String runId, String shardId, String status, String lastCursor,
                      String lastPk, int shardAttempt,
                      long rowsIn, long rowsOut, long rowsFiltered, long rowsFailed, long rowsDropped,
                      long rowsDeduped, String leaseOwner, String errorClass, String errorDetail) {
        return jdbcTemplate.update(UPSERT, taskId, runId, shardId, status, shardAttempt, lastCursor, lastPk,
                rowsIn, rowsOut, rowsFiltered, rowsFailed, rowsDropped, rowsDeduped,
                leaseOwner, errorClass, errorDetail);
    }

    /**
     * 显式连接入口（WP-02 同库同事务）：用于调用方已持有事务连接的场景，避免 JdbcTemplate
     * 在无事务上下文时另取连接。leaseOwner 为 null 时保持旧行为（不改变已有 owner）。
     */
    public int upsert(Connection conn, String taskId, String runId, String shardId, String status,
                      String lastCursor, String lastPk, int shardAttempt,
                      long rowsIn, long rowsOut, long rowsFiltered, long rowsFailed, long rowsDropped,
                      long rowsDeduped, String errorClass, String errorDetail) throws SQLException {
        return upsert(conn, taskId, runId, shardId, status, lastCursor, lastPk, shardAttempt,
                rowsIn, rowsOut, rowsFiltered, rowsFailed, rowsDropped, rowsDeduped,
                null, errorClass, errorDetail);
    }

    /** 显式连接入口：带 leaseOwner（WP-11 生产路径）。 */
    public int upsert(Connection conn, String taskId, String runId, String shardId, String status,
                      String lastCursor, String lastPk, int shardAttempt,
                      long rowsIn, long rowsOut, long rowsFiltered, long rowsFailed, long rowsDropped,
                      long rowsDeduped, String leaseOwner, String errorClass, String errorDetail)
            throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(UPSERT)) {
            ps.setString(1, taskId);
            ps.setString(2, runId);
            ps.setString(3, shardId);
            ps.setString(4, status);
            ps.setInt(5, shardAttempt);
            ps.setString(6, lastCursor);
            ps.setString(7, lastPk);
            ps.setLong(8, rowsIn);
            ps.setLong(9, rowsOut);
            ps.setLong(10, rowsFiltered);
            ps.setLong(11, rowsFailed);
            ps.setLong(12, rowsDropped);
            ps.setLong(13, rowsDeduped);
            ps.setString(14, leaseOwner);
            ps.setString(15, errorClass);
            ps.setString(16, errorDetail);
            return ps.executeUpdate();
        }
    }

    /** lease 续约（WP-11）：仅当仍由本 worker/attempt 持有。返回 0 表示 lease 已被接管。 */
    public int renewLease(String taskId, String runId, String shardId, String leaseOwner,
                          int shardAttempt, Timestamp leaseUntil) {
        return jdbcTemplate.update(RENEW_LEASE, leaseUntil, taskId, runId, shardId, shardAttempt, leaseOwner);
    }
}
