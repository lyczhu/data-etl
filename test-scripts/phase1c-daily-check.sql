-- Phase 1C 灰度日报（sz-data, data_source_id=10）
-- 用法：Get-Content test-scripts/phase1c-daily-check.sql | docker exec -i postgres psql -U postgres -d etl_system
-- 说明：run 计数列是 checkpoint 聚合的绝对镜像；G-4 平衡用 checkpoint 四段口径校验。

\echo '== 1. Run summary (last 24h, G-4 balance) =='
SELECT r.run_id,
       r.status,
       r.shard_total,
       r.shard_succeeded,
       r.shard_failed,
       r.rows_in       AS run_rows_in,
       r.rows_out      AS run_rows_out,
       r.rows_deduped  AS run_rows_deduped,
       c.ckpt_in,
       c.ckpt_out,
       c.ckpt_filtered,
       c.ckpt_failed,
       c.ckpt_deduped,
       (c.ckpt_out + c.ckpt_filtered + c.ckpt_failed + c.ckpt_deduped = c.ckpt_in) AS balance_ok,
       r.started_at,
       r.finished_at,
       EXTRACT(EPOCH FROM (COALESCE(r.finished_at, now()) - r.started_at))::bigint AS duration_seconds
  FROM etl_task_run r
  JOIN etl_task t ON t.task_id = r.task_id
  JOIN (SELECT run_id,
               COALESCE(SUM(rows_in), 0)       AS ckpt_in,
               COALESCE(SUM(rows_out), 0)      AS ckpt_out,
               COALESCE(SUM(rows_filtered), 0) AS ckpt_filtered,
               COALESCE(SUM(rows_failed), 0)   AS ckpt_failed,
               COALESCE(SUM(rows_deduped), 0)  AS ckpt_deduped
          FROM etl_task_checkpoint
         GROUP BY run_id) c ON c.run_id = r.run_id
 WHERE t.data_source_id = 10
   AND r.created_at > now() - interval '1 day'
 ORDER BY r.created_at DESC;

\echo '== 2. Orphan runs (non-terminal, no active shard, stale 5m) =='
SELECT r.task_id, r.run_id, r.status, r.last_progress_ts
  FROM etl_task_run r
 WHERE r.is_current
   AND r.status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'SUPERSEDED')
   AND NOT EXISTS (SELECT 1 FROM etl_task_checkpoint c
                    WHERE c.run_id = r.run_id AND c.status IN ('ASSIGNED', 'RUNNING'))
   AND (r.last_progress_ts IS NULL OR r.last_progress_ts < now() - interval '5 minutes');

\echo '== 3. Unresolved DLT =='
SELECT COUNT(*) AS unresolved_dlt FROM etl_task_dlt
 WHERE resolved_at IS NULL AND abandoned = false;

\echo '== 4. sz-data task status distribution =='
SELECT status, COUNT(*) AS tasks
  FROM etl_task
 WHERE data_source_id = 10
 GROUP BY status
 ORDER BY status;

\echo '== 5. Target table checks (run manually per run) =='
-- 目标表名 = source_table + '_' + task_id 前 8 位，例如 building_226710d8。
-- 每日至少校验：count(*)、count(distinct idempotency_key)、min/max(主键)、sum(主键) 与源表一致。
