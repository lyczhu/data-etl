-- ============================================================================
-- 07-etl-task-dlt.sql
-- 行级死信表（WP-04/WP-05）：FATAL/PERSISTENT 行与分片错误落表，供人工处置与告警。
-- 与 Kafka etl.task.dlt 主题互补：主题负责触发 NEEDS_ATTENTION，本表负责可查询的原文与状态。
-- 权威定义见 docs/architecture/orchestrator-design.md §4.2（Phase 1A 提前落地）。
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl_task_dlt (
    id            BIGSERIAL PRIMARY KEY,
    task_id       VARCHAR(64) NOT NULL,
    run_id        VARCHAR(64) NOT NULL,
    shard_id      VARCHAR(32),
    error_class   VARCHAR(24) NOT NULL,              -- TRANSIENT | PERSISTENT | FATAL
    error_message TEXT,
    raw_payload   TEXT,                              -- 失败行/消息原文（截断 64KB）
    resolved_at   TIMESTAMP,                         -- 人工确认已处理
    abandoned     BOOLEAN     NOT NULL DEFAULT FALSE,-- 确认放弃（宽恕数据丢失）
    created_at    TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE  etl_task_dlt IS 'ETL 行级死信；未解决行（resolved_at IS NULL AND NOT abandoned）阻止 SUCCEEDED';
COMMENT ON COLUMN etl_task_dlt.raw_payload IS '失败行原文，落表前截断到 64KB，避免大字段拖垮 DB';

CREATE INDEX IF NOT EXISTS idx_dlt_run ON etl_task_dlt (run_id, id);
CREATE INDEX IF NOT EXISTS idx_dlt_unresolved ON etl_task_dlt (run_id)
    WHERE resolved_at IS NULL AND NOT abandoned;
