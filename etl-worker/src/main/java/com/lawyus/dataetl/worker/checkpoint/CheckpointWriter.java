package com.lawyus.dataetl.worker.checkpoint;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

/**
 * checkpoint 直写器（worker 是 {@code etl_task_checkpoint} 的唯一写者，写权矩阵 §4.1）。
 *
 * <p>与目标表写入同处 worker 侧；{@code ON CONFLICT (task_id,run_id,shard_id)} 命中
 * {@code ux_checkpoint_shard}，计数用 {@code GREATEST} 保证 {@code rows_*} 单调不减（INV-3），
 * 断点续传不因重放回退。
 */
@Component
public class CheckpointWriter {

    private static final String UPSERT =
            "INSERT INTO etl_task_checkpoint "
            + "(task_id, run_id, shard_id, status, last_cursor, rows_in, rows_out, rows_dropped, rows_deduped, "
            + " error_class, error_detail, updated_at) "
            + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, now()) "
            + "ON CONFLICT (task_id, run_id, shard_id) DO UPDATE SET "
            + "  status = EXCLUDED.status, "
            + "  last_cursor = COALESCE(EXCLUDED.last_cursor, etl_task_checkpoint.last_cursor), "
            + "  rows_in = GREATEST(etl_task_checkpoint.rows_in, EXCLUDED.rows_in), "
            + "  rows_out = GREATEST(etl_task_checkpoint.rows_out, EXCLUDED.rows_out), "
            + "  rows_dropped = GREATEST(etl_task_checkpoint.rows_dropped, EXCLUDED.rows_dropped), "
            + "  rows_deduped = GREATEST(etl_task_checkpoint.rows_deduped, EXCLUDED.rows_deduped), "
            + "  error_class = EXCLUDED.error_class, "
            + "  error_detail = EXCLUDED.error_detail, "
            + "  updated_at = now()";

    private final JdbcTemplate jdbcTemplate;

    public CheckpointWriter(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void upsert(String taskId, String runId, String shardId, String status,
                       String lastCursor, long rowsIn, long rowsOut, long rowsDropped, long rowsDeduped,
                       String errorClass, String errorDetail) {
        jdbcTemplate.update(UPSERT, taskId, runId, shardId, status, lastCursor,
                rowsIn, rowsOut, rowsDropped, rowsDeduped, errorClass, errorDetail);
    }
}
