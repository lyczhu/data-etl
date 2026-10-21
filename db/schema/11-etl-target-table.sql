-- ============================================================================
-- 11-etl-target-table.sql
-- Phase 2E：目标表登记与生命周期治理。
--
-- 作用：
--   1) 冲突检查：同一目标库上的表名只能登记给一个 task；
--   2) 孤儿识别：task 已删除或长期无更新 → ORPHANED；
--   3) 清理审计：只允许清理 ORPHANED，PURGED 保留表名与时间，便于追溯。
-- target_data_source_id 为 NULL 表示写 orchestrator 本地库；唯一索引用 0 折叠 NULL。
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl_target_table (
    id                    BIGSERIAL PRIMARY KEY,
    task_id               VARCHAR(64)  NOT NULL,
    target_data_source_id BIGINT,
    table_name            VARCHAR(100) NOT NULL,
    state                 VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE',
    created_at            TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_used_at          TIMESTAMP,
    purged_at             TIMESTAMP,
    CONSTRAINT ck_target_table_state CHECK (state IN ('ACTIVE','ORPHANED','PURGED'))
);

COMMENT ON TABLE etl_target_table IS '目标表登记：同一目标库表名唯一归属一个 task';
COMMENT ON COLUMN etl_target_table.state IS 'ACTIVE 使用中 | ORPHANED 待清理 | PURGED 已清理';

CREATE UNIQUE INDEX IF NOT EXISTS ux_target_scope
    ON etl_target_table (COALESCE(target_data_source_id, 0), table_name);
CREATE INDEX IF NOT EXISTS idx_target_task ON etl_target_table (task_id);
CREATE INDEX IF NOT EXISTS idx_target_state ON etl_target_table (state);
