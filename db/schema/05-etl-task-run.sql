-- ============================================================================
-- 05-etl-task-run.sql
-- ETL 执行实例（一次执行意图），run 层状态机的宿主。
-- 权威定义见 docs/architecture/orchestrator-design.md §4.2。
--
-- 注意：spring.jpa.hibernate.ddl-auto=update 不会创建部分唯一索引与 CHECK 约束，
-- 因此本脚本必须手工执行，不得依赖 Hibernate（Phase 1 结束后建议降为 validate）。
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl_task_run (
    id                  BIGSERIAL PRIMARY KEY,
    run_id              VARCHAR(64)  NOT NULL UNIQUE,           -- UUID，D-21
    task_id             VARCHAR(64)  NOT NULL REFERENCES etl_task(task_id) ON DELETE CASCADE,
    attempt_no          INT          NOT NULL DEFAULT 1,        -- 同一 run 内任务级重试次数
    status              VARCHAR(20)  NOT NULL,                  -- EtlTaskStatus 枚举名
    is_current          BOOLEAN      NOT NULL DEFAULT TRUE,
    queued_reason       VARCHAR(20),                            -- SCHEDULED | THROTTLED
    shard_total         INT          NOT NULL DEFAULT 0,
    shard_succeeded     INT          NOT NULL DEFAULT 0,
    shard_failed        INT          NOT NULL DEFAULT 0,
    shard_skipped       INT          NOT NULL DEFAULT 0,
    rows_in             BIGINT       NOT NULL DEFAULT 0,
    rows_out            BIGINT       NOT NULL DEFAULT 0,
    rows_dropped        BIGINT       NOT NULL DEFAULT 0,        -- 与 etl_records_dropped_total 同口径（D-08）
    rows_deduped        BIGINT       NOT NULL DEFAULT 0,        -- ON CONFLICT 命中，不参与对账等式
    next_attempt_at     TIMESTAMP,                              -- RETRYING 退避到期时间
    dispatched_at       TIMESTAMP,                              -- 心跳超时判定基线
    last_progress_ts    TIMESTAMP,                              -- 卡死判定基线
    cancel_requested_at TIMESTAMP,                              -- 软取消标记（D-18）
    started_at          TIMESTAMP,
    finished_at         TIMESTAMP,
    error_summary       TEXT,                                   -- 首个致命异常 stack 前 2KB + shard 列表
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_run_status CHECK (status IN (
        'CREATED','VALIDATED','QUEUED','DISPATCHED','RUNNING','PARTIAL_SUCCESS',
        'RETRYING','NEEDS_ATTENTION','SUCCEEDED','FAILED','CANCELLED','SUPERSEDED'))
);

COMMENT ON TABLE  etl_task_run IS 'ETL 执行实例（一次执行意图），状态机宿主';
COMMENT ON COLUMN etl_task_run.attempt_no   IS '同一 run 内的任务级重试序号；新建 run 归 1';
COMMENT ON COLUMN etl_task_run.rows_deduped IS '幂等命中数（重放场景），不计入对账等式';

-- INV-1：同一 task 至多一个非终态当前 run，交 DB 强制（P3）
CREATE UNIQUE INDEX IF NOT EXISTS ux_run_task_current
    ON etl_task_run (task_id)
    WHERE is_current AND status NOT IN ('SUCCEEDED','FAILED','CANCELLED','SUPERSEDED');

-- INV-7：同一 task 的 attempt_no 唯一（防并发双自增）
CREATE UNIQUE INDEX IF NOT EXISTS ux_run_task_attempt
    ON etl_task_run (task_id, attempt_no);

CREATE INDEX IF NOT EXISTS idx_run_status_progress ON etl_task_run (status, last_progress_ts);
CREATE INDEX IF NOT EXISTS idx_run_dispatched ON etl_task_run (status, dispatched_at);
