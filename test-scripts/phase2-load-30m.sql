-- ============================================================================
-- phase2-load-30m.sql
-- Phase 2A 容量基线：30M 行主表 + 100k 行并发表。
-- 在 etl_system 库执行（docker exec -i postgres psql -U postgres -d etl_system）。
-- 注意：脚本会 DROP 并重建两张压测表，仅用于测试环境。
-- ============================================================================

DROP TABLE IF EXISTS p2_load_30m;
CREATE TABLE p2_load_30m (
    id         BIGINT PRIMARY KEY,
    name       VARCHAR(64),
    amount     NUMERIC(12, 2),
    created_at TIMESTAMP
);

INSERT INTO p2_load_30m (id, name, amount, created_at)
SELECT g,
       'load-' || g,
       (g % 10000) / 100.0,
       TIMESTAMP '2026-01-01 00:00:00' + (g || ' seconds')::interval
FROM generate_series(1, 30000000) AS g;

ANALYZE p2_load_30m;

DROP TABLE IF EXISTS p2_load_100k;
CREATE TABLE p2_load_100k (
    id         BIGINT PRIMARY KEY,
    name       VARCHAR(64),
    amount     NUMERIC(12, 2),
    created_at TIMESTAMP
);

INSERT INTO p2_load_100k (id, name, amount, created_at)
SELECT g,
       'concurrent-' || g,
       (g % 10000) / 100.0,
       TIMESTAMP '2026-01-01 00:00:00' + (g || ' seconds')::interval
FROM generate_series(1, 100000) AS g;

ANALYZE p2_load_100k;

SELECT 'p2_load_30m' AS table_name, COUNT(*) AS rows FROM p2_load_30m
UNION ALL
SELECT 'p2_load_100k', COUNT(*) FROM p2_load_100k;
