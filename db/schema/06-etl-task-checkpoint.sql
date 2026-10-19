-- ============================================================================
-- 06-etl-task-checkpoint.sql
-- 分片断点表，worker 直写（写权矩阵 §4.1），断点续传唯一真源。
-- 权威定义见 docs/architecture/orchestrator-design.md §4.2 / §4.3。
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl_task_checkpoint (
    id            BIGSERIAL PRIMARY KEY,
    task_id       VARCHAR(64) NOT NULL,
    run_id        VARCHAR(64) NOT NULL REFERENCES etl_task_run(run_id) ON DELETE CASCADE,
    shard_id      VARCHAR(32) NOT NULL,
    status        VARCHAR(16) NOT NULL DEFAULT 'PENDING',   -- EtlShardStatus
    shard_attempt INT         NOT NULL DEFAULT 0,           -- 分片级重试次数（与 run.attempt_no 解耦，D-11）
    last_cursor   TEXT,                                     -- JDBC: pk 上界；Mongo: _id；File: 行区间+offset；Stream: topic-partition:offset
    last_pk       VARCHAR(128),
    rows_in       BIGINT      NOT NULL DEFAULT 0,           -- 累计单调不减（D-08 / INV-3）
    rows_out      BIGINT      NOT NULL DEFAULT 0,
    rows_dropped  BIGINT      NOT NULL DEFAULT 0,
    rows_deduped  BIGINT      NOT NULL DEFAULT 0,
    error_class   VARCHAR(24),                              -- TRANSIENT | PERSISTENT | FATAL
    error_detail  TEXT,
    updated_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_checkpoint_status CHECK (
        status IN ('PENDING','ASSIGNED','RUNNING','SUCCEEDED','FAILED','SKIPPED')),
    -- 对账等式下推到行级：单分片绝不可能写出比读取更多的行（INV-4 的源头兜底）
    CONSTRAINT ck_checkpoint_rows CHECK (rows_out + rows_dropped <= rows_in)
);

COMMENT ON COLUMN etl_task_checkpoint.rows_in IS '跨 attempt 累计实读行数，禁止覆盖式回退';

-- INV-2：(task_id, run_id, shard_id) 唯一
CREATE UNIQUE INDEX IF NOT EXISTS ux_checkpoint_shard ON etl_task_checkpoint (task_id, run_id, shard_id);
CREATE INDEX IF NOT EXISTS idx_checkpoint_run_status ON etl_task_checkpoint (run_id, status);
