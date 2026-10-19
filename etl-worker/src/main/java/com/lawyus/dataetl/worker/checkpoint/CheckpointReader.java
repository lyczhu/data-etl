package com.lawyus.dataetl.worker.checkpoint;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * checkpoint 读侧（WP-02 续跑）：worker 只读本 run/shard 的游标与累计计数，
 * 用于“重试从 last_cursor 之后继续”和“跨 attempt 累计计数”。
 */
@Component
public class CheckpointReader {

    private final JdbcTemplate jdbcTemplate;

    public CheckpointReader(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    /** 读取 state；行不存在（首次派工）返回 {@link ShardCheckpointState#empty()}。 */
    public ShardCheckpointState load(String taskId, String runId, String shardId) {
        var rows = jdbcTemplate.queryForList(
                "SELECT status, shard_attempt, last_cursor, last_pk, rows_in, rows_out, "
                        + "rows_filtered, rows_failed, rows_dropped, rows_deduped "
                        + "FROM etl_task_checkpoint WHERE task_id = ? AND run_id = ? AND shard_id = ?",
                taskId, runId, shardId);
        if (rows.isEmpty()) {
            return ShardCheckpointState.empty();
        }
        var r = rows.get(0);
        return new ShardCheckpointState(
                (String) r.get("status"),
                intOf(r.get("shard_attempt")),
                (String) r.get("last_cursor"),
                (String) r.get("last_pk"),
                longOf(r.get("rows_in")),
                longOf(r.get("rows_out")),
                longOf(r.get("rows_filtered")),
                longOf(r.get("rows_failed")),
                longOf(r.get("rows_dropped")),
                longOf(r.get("rows_deduped")));
    }

    private static int intOf(Object v) {
        return v instanceof Number n ? n.intValue() : 0;
    }

    private static long longOf(Object v) {
        return v instanceof Number n ? n.longValue() : 0L;
    }
}
