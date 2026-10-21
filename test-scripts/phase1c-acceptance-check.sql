-- ============================================================================
-- Phase 1C 接入测试验收（Gate 3）
-- 一次输出 8 项结论，ok 全为 t 才算该轮通过。口径见
-- docs/architecture/etl-architecture-evolution-plan.md「Phase 1C · 灰度接入测试」。
--
-- 用法（PowerShell，目标表位于 etl_system 时）：
--   Get-Content test-scripts/phase1c-acceptance-check.sql -Raw |
--     docker exec -i postgres psql -U postgres -d etl_system `
--       -v run_id=47021610-6154-4903-b50e-cbdc64b2421a `
--       -v target_table=building_226710d8 `
--       -v src_count=650000 -v src_min=1 -v src_max=650000 -v src_sum=211250325000
--
-- 参数：
--   run_id         必填，etl_task_run.run_id
--   target_table   必填，目标表名；目标库不是 etl_system 时，第 1/2/4 项请在目标库执行
--   src_count/min/max/sum  必填，来自源表的独立查询。不要用 checkpoint 反推，否则是自证。
--
-- 假设：单列数值主键（分片区间 pk_from/pk_to 的同一前提）；第 3 项的区间衔接检查仅适用 JDBC 源。
-- 告警项（P0 firing）无法用 SQL 判定，需在 Prometheus 上确认 ALERTS{alertstate="firing"} 为空。
-- ============================================================================
\set ON_ERROR_STOP on

\if :{?run_id}
\else
  \echo 'ERROR: 缺少 -v run_id=<run_id>'
  \q
\endif

\if :{?target_table}
\else
  \echo 'ERROR: 缺少 -v target_table=<目标表名>'
  \q
\endif

\if :{?src_count}
\else
  \echo 'ERROR: 缺少 -v src_count=<-源表行数->'
  \q
\endif

\if :{?src_min}
\else
  \echo 'ERROR: 缺少 -v src_min=<-源表最小主键->'
  \q
\endif

\if :{?src_max}
\else
  \echo 'ERROR: 缺少 -v src_max=<-源表最大主键->'
  \q
\endif

\if :{?src_sum}
\else
  \echo 'ERROR: 缺少 -v src_sum=<-源表主键之和->'
  \q
\endif

-- run 不存在时直接失败，避免第 7 项在空集上空转通过
SELECT count(*) AS p1c_run_exists FROM etl_task_run WHERE run_id = :'run_id' \gset
\if :p1c_run_exists
\else
  \echo 'ERROR: run_id 不存在'
  \q
\endif

DROP TABLE IF EXISTS p1c_accept;
CREATE TEMP TABLE p1c_accept (
    no       INT,
    item     TEXT,
    expected TEXT,
    actual   TEXT,
    ok       BOOLEAN
);

-- checkpoint 聚合：G-4 与镜像列都以它为唯一口径
CREATE TEMP TABLE p1c_ckpt AS
SELECT COALESCE(SUM(rows_in), 0)       AS ckpt_in,
       COALESCE(SUM(rows_out), 0)      AS ckpt_out,
       COALESCE(SUM(rows_filtered), 0) AS ckpt_filtered,
       COALESCE(SUM(rows_failed), 0)   AS ckpt_failed,
       COALESCE(SUM(rows_dropped), 0)  AS ckpt_dropped,
       COALESCE(SUM(rows_deduped), 0)  AS ckpt_deduped
  FROM etl_task_checkpoint
 WHERE run_id = :'run_id';

\echo '== run / task 基本信息 =='
SELECT r.run_id,
       r.task_id,
       t.data_source_id,
       t.source_table_name,
       t.target_table_name,
       r.status,
       r.shard_total,
       r.shard_succeeded,
       r.shard_failed,
       r.shard_skipped,
       r.rows_in,
       r.rows_out,
       r.rows_dropped,
       r.rows_deduped,
       EXTRACT(EPOCH FROM (r.finished_at - r.started_at))::bigint AS duration_seconds
  FROM etl_task_run r
  JOIN etl_task t ON t.task_id = r.task_id
 WHERE r.run_id = :'run_id';

-- 1. 目标表行数 = 源表行数
SELECT format(
  'INSERT INTO p1c_accept (no, item, expected, actual, ok) '
  'SELECT 1, %L, %L, count(*)::text, count(*) = %s::bigint FROM %I',
  '1. 目标表行数 = 源表行数', :'src_count', :src_count, :'target_table'
) \gexec

-- 2. 主键 min/max/sum 源目标一致
SELECT format(
  'INSERT INTO p1c_accept (no, item, expected, actual, ok) '
  'SELECT 2, %L, %L, '
  'min_id::text || ''/'' || max_id::text || ''/'' || sum_id::text, '
  '(min_id = %s::numeric AND max_id = %s::numeric AND sum_id = %s::numeric) '
  'FROM (SELECT min(id) AS min_id, max(id) AS max_id, sum(id) AS sum_id FROM %I) t',
  '2. 主键 min/max/sum 与源表一致',
  :'src_min' || '/' || :'src_max' || '/' || :'src_sum',
  :src_min, :src_max, :src_sum, :'target_table'
) \gexec

-- 3. 分片覆盖：Σrows_in = 源行数，PK 区间首尾衔接、无缝无叠
INSERT INTO p1c_accept (no, item, expected, actual, ok)
SELECT 3,
       '3. 分片覆盖：Σrows_in = 源行数，PK 区间首尾衔接无缝无叠',
       :'src_count' || ' 行 / ' || :'src_min' || '..' || :'src_max',
       c.ckpt_in || ' 行 / ' || COALESCE(g.min_from::text, 'null') || '..' || COALESCE(g.max_to::text, 'null')
         || ' / gaps=' || g.gaps || ' / shards=' || g.shards || '/' || r.shard_total,
       (c.ckpt_in = :src_count
        AND g.min_from = :src_min
        AND g.max_to = :src_max
        AND g.gaps = 0
        AND g.shards = r.shard_total)
  FROM p1c_ckpt c,
       etl_task_run r,
       (SELECT count(*) AS shards,
               min(pk_from) AS min_from,
               max(pk_to)   AS max_to,
               count(*) FILTER (WHERE prev_to IS NOT NULL AND prev_to + 1 <> pk_from) AS gaps
          FROM (SELECT pk_from, pk_to, lag(pk_to) OVER (ORDER BY pk_from) AS prev_to
                  FROM etl_task_checkpoint
                 WHERE run_id = :'run_id') x) g
 WHERE r.run_id = :'run_id';

-- 4. 幂等键唯一数 = 目标行数
SELECT format(
  'INSERT INTO p1c_accept (no, item, expected, actual, ok) '
  'SELECT 4, %L, %L, '
  '(SELECT count(*)::text || '' rows / '' || count(DISTINCT idempotency_key) || '' keys'' FROM %I), '
  '(SELECT count(*) = count(DISTINCT idempotency_key) FROM %I)',
  '4. 幂等键唯一数 = 目标行数', 'count(*) = count(distinct idempotency_key)',
  :'target_table', :'target_table'
) \gexec

-- 5. G-4 四段平衡 + run 镜像列 = checkpoint 聚合
INSERT INTO p1c_accept (no, item, expected, actual, ok)
SELECT 5,
       '5. G-4 四段平衡且 run 镜像列 = checkpoint 聚合',
       'out+filtered+failed+deduped = in',
       format('ckpt %s+%s+%s+%s vs %s；mirror in/out/dropped/deduped=%s/%s/%s/%s',
              c.ckpt_out, c.ckpt_filtered, c.ckpt_failed, c.ckpt_deduped, c.ckpt_in,
              r.rows_in, r.rows_out, r.rows_dropped, r.rows_deduped),
       (c.ckpt_out + c.ckpt_filtered + c.ckpt_failed + c.ckpt_deduped = c.ckpt_in)
       AND r.rows_in = c.ckpt_in
       AND r.rows_out = c.ckpt_out
       AND r.rows_dropped = c.ckpt_dropped
       AND r.rows_deduped = c.ckpt_deduped
  FROM p1c_ckpt c,
       etl_task_run r
 WHERE r.run_id = :'run_id';

-- 6. run 终态且全部分片成功
INSERT INTO p1c_accept (no, item, expected, actual, ok)
SELECT 6,
       '6. run 终态且全部分片成功',
       'SUCCEEDED, failed=0, skipped=0',
       r.status || format(' (%s/%s succeeded, failed=%s, skipped=%s)',
                          r.shard_succeeded, r.shard_total, r.shard_failed, r.shard_skipped),
       r.status = 'SUCCEEDED'
       AND r.shard_succeeded = r.shard_total
       AND r.shard_failed = 0
       AND r.shard_skipped = 0
  FROM etl_task_run r
 WHERE r.run_id = :'run_id';

-- 7. 同 dataSource 孤儿 run = 0
INSERT INTO p1c_accept (no, item, expected, actual, ok)
SELECT 7, '7. 同 dataSource 孤儿 run = 0', '0', count(*)::text, count(*) = 0
  FROM etl_task_run r
  JOIN etl_task t ON t.task_id = r.task_id
 WHERE r.is_current
   AND t.data_source_id = (SELECT t2.data_source_id
                             FROM etl_task_run r2
                             JOIN etl_task t2 ON t2.task_id = r2.task_id
                            WHERE r2.run_id = :'run_id')
   AND r.status NOT IN ('SUCCEEDED', 'FAILED', 'CANCELLED', 'SUPERSEDED')
   AND NOT EXISTS (SELECT 1 FROM etl_task_checkpoint c
                    WHERE c.run_id = r.run_id AND c.status IN ('ASSIGNED', 'RUNNING'))
   AND (r.last_progress_ts IS NULL OR r.last_progress_ts < now() - interval '5 minutes');

-- 8. 本 run 未解决 DLT = 0
INSERT INTO p1c_accept (no, item, expected, actual, ok)
SELECT 8, '8. 本 run 未解决 DLT = 0', '0', count(*)::text, count(*) = 0
  FROM etl_task_dlt
 WHERE run_id = :'run_id'
   AND resolved_at IS NULL
   AND abandoned = FALSE;

\echo '== Gate 3 验收结论 =='
SELECT no, item, expected, actual, ok FROM p1c_accept ORDER BY no;

\echo '== 汇总（ok 全为 t 才算通过）=='
SELECT count(*)                            AS checks,
       count(*) FILTER (WHERE ok)          AS passed,
       count(*) FILTER (WHERE ok IS NOT TRUE) AS failed
  FROM p1c_accept;

\echo '== 参考：全库未解决 DLT（不参与本 run 判定）=='
SELECT count(*) AS unresolved_dlt_global
  FROM etl_task_dlt
 WHERE resolved_at IS NULL AND abandoned = FALSE;

\echo '== 参考：源库分段校验 SQL（复制到源库执行，逐段与 checkpoint 的 rows_in/rows_out 对照）=='
SELECT string_agg(
         format('SELECT %L AS shard, count(*) AS c, sum(id) AS s FROM %I WHERE id BETWEEN %s AND %s',
                c.shard_id, t.source_table_name, c.pk_from, c.pk_to),
         ' UNION ALL ' ORDER BY c.shard_id) AS source_check_sql
  FROM etl_task_checkpoint c
  JOIN etl_task_run r ON r.run_id = c.run_id
  JOIN etl_task t ON t.task_id = r.task_id
 WHERE c.run_id = :'run_id'
   AND c.pk_from IS NOT NULL;

\echo '== 提醒：P0 告警需在 Prometheus 上确认 ALERTS{alertstate="firing"} 为空 =='
