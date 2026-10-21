-- ============================================================================
-- 10-etl-reconciliation.sql
-- Phase 2D：每日对账结果。
--
-- 口径（orchestrator-design.md §12.2 check 2/3）：
--   SUCCEEDED：source_rows - rows_filtered - rows_failed = target_rows
--              且 target_rows = COUNT(DISTINCT idempotency_key)
--   其它终态：target_rows = checkpoint SUM(rows_out)，且无重复幂等键
--   四段平衡：rows_in = rows_out + rows_filtered + rows_failed + rows_deduped
-- 每个 run 一条记录（UNIQUE run_id），夜间任务跳过已存在记录；人工可 force 重算。
-- ============================================================================

CREATE TABLE IF NOT EXISTS etl_reconciliation_run (
    id                   BIGSERIAL PRIMARY KEY,
    run_id               VARCHAR(64) NOT NULL UNIQUE,
    task_id              VARCHAR(64) NOT NULL,
    status               VARCHAR(16) NOT NULL,      -- PASS | MISMATCH | ERROR
    source_rows          BIGINT,
    filtered_rows        BIGINT,
    failed_rows          BIGINT,
    deduped_rows         BIGINT,
    rows_in              BIGINT,
    rows_out             BIGINT,
    target_rows          BIGINT,
    distinct_idempotency BIGINT,
    expected_target_rows BIGINT,
    diff                 BIGINT,
    details              TEXT,
    created_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT ck_reconciliation_status CHECK (status IN ('PASS','MISMATCH','ERROR'))
);

COMMENT ON TABLE etl_reconciliation_run IS '每日对账结果：每个终态 run 一条（UNIQUE run_id）';
COMMENT ON COLUMN etl_reconciliation_run.expected_target_rows IS 'SUCCEEDED 用 source-filtered-failed；其它终态用 rows_out';
COMMENT ON COLUMN etl_reconciliation_run.distinct_idempotency IS 'target 表 COUNT(DISTINCT idempotency_key)';

CREATE INDEX IF NOT EXISTS idx_recon_task ON etl_reconciliation_run (task_id, id);
CREATE INDEX IF NOT EXISTS idx_recon_status ON etl_reconciliation_run (status, created_at);
