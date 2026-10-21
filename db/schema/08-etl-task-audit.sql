-- ============================================================================
-- 08-etl-task-audit.sql
-- Phase 2B：状态迁移审计（INV-9 / orchestrator-design.md §4.1）。
--
-- 规则：每一次真正落地的状态迁移（含人工操作、新建 run）恰好一条审计；
-- CAS 失败/非法迁移/guard 拦截不写。审计与状态迁移同事务写入（P7）。
-- 审计不可被任务/run 级联删除，因此不建外键。
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl_task_audit (
    id          BIGSERIAL PRIMARY KEY,
    task_id     VARCHAR(64)  NOT NULL,
    run_id      VARCHAR(64),
    from_status VARCHAR(24)  NOT NULL,              -- 新建 run 用 '(none)'
    to_status   VARCHAR(24)  NOT NULL,
    actor       VARCHAR(64)  NOT NULL,              -- system:submit / orchestrator:dispatch / api:command / sweeper:...
    reason      VARCHAR(256),
    error_class VARCHAR(24),                        -- TRANSIENT | PERSISTENT | FATAL | FATAL_CONFIG
    context     TEXT,                               -- TransitionContext 关键字段的 JSON 快照
    created_at  TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE etl_task_audit IS '状态迁移审计（INV-9）：每次成功迁移一条，与状态写同事务';
COMMENT ON COLUMN etl_task_audit.from_status IS '迁移前状态；新建 run 记为 (none)';
COMMENT ON COLUMN etl_task_audit.context IS '上下文 JSON 快照（shardId/attemptNo/nextAttemptAt 等）';

CREATE INDEX IF NOT EXISTS idx_audit_task ON etl_task_audit (task_id, id);
CREATE INDEX IF NOT EXISTS idx_audit_run ON etl_task_audit (run_id, id);
CREATE INDEX IF NOT EXISTS idx_audit_created ON etl_task_audit (created_at);
