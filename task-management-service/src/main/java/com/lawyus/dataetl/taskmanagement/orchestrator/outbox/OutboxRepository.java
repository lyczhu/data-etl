package com.lawyus.dataetl.taskmanagement.orchestrator.outbox;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.sql.Timestamp;
import java.util.List;

/**
 * {@code etl_outbox} 的 JDBC 访问（Phase 2C）。
 *
 * <p>不使用 JPA：outbox 是高写入、低延迟的基础设施表，SQL 直写更可控；
 * {@link #insert} 由 {@code DispatchService.dispatchOne} 在状态迁移事务内调用。
 */
@Repository
public class OutboxRepository {

    private final JdbcTemplate jdbc;

    public OutboxRepository(JdbcTemplate jdbc) {
        this.jdbc = jdbc;
    }

    public void insert(String topic, String messageKey, String payload) {
        jdbc.update("INSERT INTO etl_outbox (topic, message_key, payload) VALUES (?, ?, ?)",
                topic, messageKey, payload);
    }

    public List<OutboxRow> findDue(int limit) {
        return jdbc.query(
                "SELECT id, topic, message_key, payload, attempt_no, created_at "
                        + "FROM etl_outbox WHERE sent_at IS NULL AND next_attempt_at <= now() "
                        + "ORDER BY id LIMIT ?",
                (rs, rowNum) -> new OutboxRow(
                        rs.getLong("id"),
                        rs.getString("topic"),
                        rs.getString("message_key"),
                        rs.getString("payload"),
                        rs.getInt("attempt_no"),
                        rs.getTimestamp("created_at")),
                Math.max(1, limit));
    }

    public void markSent(long id) {
        jdbc.update("UPDATE etl_outbox SET sent_at = now(), last_error = NULL WHERE id = ?", id);
    }

    public void markFailed(long id, int attemptNo, long delaySeconds, String error) {
        jdbc.update("UPDATE etl_outbox SET attempt_no = ?, "
                        + "next_attempt_at = now() + make_interval(secs => ?::double precision), "
                        + "last_error = ? WHERE id = ?",
                attemptNo, delaySeconds, abbreviate(error, 512), id);
    }

    /** 已发送行保留期清理；返回删除行数。 */
    public int deleteSentOlderThanDays(int days) {
        return jdbc.update("DELETE FROM etl_outbox WHERE sent_at IS NOT NULL "
                + "AND sent_at < now() - make_interval(days => ?)", days);
    }

    /** 待投递行数（Gauge）。 */
    public long countPending() {
        Long count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM etl_outbox WHERE sent_at IS NULL", Long.class);
        return count == null ? 0L : count;
    }

    /** 最老待投递行的创建时间（Gauge）；无待投递返回 null。 */
    public Timestamp oldestPendingCreatedAt() {
        return jdbc.queryForObject(
                "SELECT MIN(created_at) FROM etl_outbox WHERE sent_at IS NULL", Timestamp.class);
    }

    private static String abbreviate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() <= max ? value : value.substring(0, max);
    }
}
