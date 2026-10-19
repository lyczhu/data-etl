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
    pk_from       BIGINT,                                    -- 分片主键闭区间下界（validate/T2 落库，供 pump 派工，§7.2）
    pk_to         BIGINT,                                    -- 分片主键闭区间上界（非 JDBC 源可为空）
    rows_in       BIGINT      NOT NULL DEFAULT 0,           -- 累计单调不减（D-08 / INV-3）
    rows_out      BIGINT      NOT NULL DEFAULT 0,
    rows_dropped  BIGINT      NOT NULL DEFAULT 0,           -- = rows_filtered + rows_failed（兼容旧口径）
    rows_filtered BIGINT      NOT NULL DEFAULT 0,           -- 业务规则过滤，可进成功等式
    rows_failed   BIGINT      NOT NULL DEFAULT 0,           -- 写入/解析失败，必须为 0 才可 SUCCEEDED
    rows_deduped  BIGINT      NOT NULL DEFAULT 0,
    lease_owner   VARCHAR(128),                              -- 当前持有该分片的 worker（WP-11 lease/fencing）
    lease_until   TIMESTAMP,                                 -- lease 到期时间；过期可被 /resume 重置
    error_class   VARCHAR(24),                              -- TRANSIENT | PERSISTENT | FATAL
    error_detail  TEXT,
    updated_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_checkpoint_status CHECK (
        status IN ('PENDING','ASSIGNED','RUNNING','SUCCEEDED','FAILED','SKIPPED')),
    -- 对账等式下推到行级：单分片绝不可能写出比读取更多的行（INV-4 的源头兜底）
    CONSTRAINT ck_checkpoint_rows CHECK (rows_out + rows_dropped <= rows_in)
);

COMMENT ON COLUMN etl_task_checkpoint.rows_in IS '跨 attempt 累计实读行数，禁止覆盖式回退';
COMMENT ON COLUMN etl_task_checkpoint.pk_from IS 'JDBC 分片主键闭区间下界，validate 计划分片时由编排器写入；worker 只读';

-- 既有库迁移：CREATE TABLE IF NOT EXISTS 不会补列（Phase 1A，幂等可重复执行）
ALTER TABLE etl_task_checkpoint ADD COLUMN IF NOT EXISTS rows_filtered BIGINT NOT NULL DEFAULT 0;
ALTER TABLE etl_task_checkpoint ADD COLUMN IF NOT EXISTS rows_failed   BIGINT NOT NULL DEFAULT 0;
ALTER TABLE etl_task_checkpoint ADD COLUMN IF NOT EXISTS lease_owner   VARCHAR(128);
ALTER TABLE etl_task_checkpoint ADD COLUMN IF NOT EXISTS lease_until   TIMESTAMP;

-- 列补齐后再写注释，避免旧库上 COMMENT 先于 ADD COLUMN 执行
COMMENT ON COLUMN etl_task_checkpoint.rows_filtered IS '业务规则过滤行数（transformer 返回 null），参与 rows_out+rows_filtered+rows_failed=rows_in';
COMMENT ON COLUMN etl_task_checkpoint.rows_failed IS '写入/解析失败行数（已落 etl_task_dlt）；>0 时禁止 SUCCEEDED';

-- INV-2：(task_id, run_id, shard_id) 唯一
CREATE UNIQUE INDEX IF NOT EXISTS ux_checkpoint_shard ON etl_task_checkpoint (task_id, run_id, shard_id);
CREATE INDEX IF NOT EXISTS idx_checkpoint_run_status ON etl_task_checkpoint (run_id, status);
CREATE INDEX IF NOT EXISTS idx_checkpoint_lease ON etl_task_checkpoint (lease_until)
    WHERE lease_until IS NOT NULL;
