# ETL 性能基线（2026-10，Phase 0）

> 状态：**已完成 legacy 链路基线采集（2026-10-19）**，实测值见 §7；`etl_task_run` 口径的 Phase 1A 复测模板仍保留在 §3。
> 目的：在 Phase 1A 改动前固定“修复后基线”，供后续 Gate 1 对比 P95、吞吐、GC、源库压力与 Kafka lag。
> 前提：`etl.routing.new-worker=false`，旧链路正常跑；本次为 4 次连续单表 20,000 行样本，不等同于 2 小时业务高峰窗口，结论只用于同口径前后对比。

---

## 1. 采集口径

| 指标 | 口径 | 来源 |
|---|---|---|
| 任务时长 P50/P95/P99 | `finished_at - started_at`，仅统计 `SUCCEEDED` 的当前 run | `etl_task_run`（Phase 1A）；legacy 链路用 `etl_task.end_time - start_time` |
| 吞吐（行/秒） | `SUM(rows_out) / SUM(duration_seconds)` | `etl_task_run`（Phase 1A）；legacy 链路用目标表行数 / 任务时长 |
| 失败率 | `FAILED / (SUCCEEDED + FAILED + PARTIAL_SUCCESS)` | `etl_task_run`（Phase 1A）；legacy 链路用 `etl_task.status` |
| Full GC 次数/停顿 | 采集窗口内 `Full GC` 次数、总停顿、最大停顿 | `logs/gc.log` |
| 源库 CPU/连接数 | 源库实例 CPU、活跃连接、慢查询数 | 源库监控 / `pg_stat_activity` / `SHOW STATUS` |
| Kafka lag | 各 consumer group 的 `LAG` 总和与最大值 | `kafka-consumer-groups --describe` |
| JVM 堆 | 老年代使用率、GC 后存活 | `/actuator/metrics/jvm.memory.used` |
| HTTP 错误率 | 5xx 比例 | gateway / actuator 指标 |

> 所有指标都记录**采集时间、采集窗口、数据量、服务版本/commit**；没有这些上下文的数字不能作为基线。

---

## 2. 服务端口与检查

| 服务 | 端口 | 健康检查 |
|---|---:|---|
| gateway | 8080 | `http://localhost:8080/actuator/health` |
| data-source | 8081 | `http://localhost:8081/actuator/health` |
| task-management | 8082 | `http://localhost:8082/actuator/health` |
| data-extraction | 8083 | `http://localhost:8083/actuator/health` |
| data-transformation | 8084 | `http://localhost:8084/actuator/health` |
| data-storage | 8085 | `http://localhost:8085/actuator/health` |
| user-service | 8086 | `http://localhost:8086/actuator/health` |
| etl-worker | 8087 | `http://localhost:8087/actuator/health` |

PowerShell 批量检查：

```powershell
8080..8087 | ForEach-Object {
  try {
    $r = Invoke-RestMethod "http://localhost:$($_)/actuator/health" -TimeoutSec 3
    [pscustomobject]@{ Port = $_; Status = $r.status }
  } catch {
    [pscustomobject]@{ Port = $_; Status = "DOWN: $($_.Exception.Message)" }
  }
} | Format-Table -AutoSize
```

---

## 3. 任务时长与吞吐

```sql
-- 最近 7 天已成功 run 的时长分位（秒）
SELECT
  percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (finished_at - started_at))) AS p50_s,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (finished_at - started_at))) AS p95_s,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (finished_at - started_at))) AS p99_s,
  COUNT(*) AS runs
FROM etl_task_run
WHERE status = 'SUCCEEDED'
  AND is_current = TRUE
  AND started_at IS NOT NULL
  AND finished_at IS NOT NULL
  AND finished_at > now() - interval '7 days';

-- 吞吐（行/秒）
SELECT
  SUM(rows_out) AS rows_out,
  SUM(EXTRACT(EPOCH FROM (finished_at - started_at))) AS duration_s,
  CASE WHEN SUM(EXTRACT(EPOCH FROM (finished_at - started_at))) > 0
       THEN SUM(rows_out) / SUM(EXTRACT(EPOCH FROM (finished_at - started_at)))
       ELSE 0 END AS rows_per_second
FROM etl_task_run
WHERE status = 'SUCCEEDED'
  AND is_current = TRUE
  AND started_at IS NOT NULL
  AND finished_at IS NOT NULL
  AND finished_at > now() - interval '7 days';
```

执行方式（把上面的 SQL 保存为 `baseline.sql`；本机 PostgreSQL 5433，容器名按实际调整）：

```powershell
Get-Content .\baseline.sql | docker exec -i postgres psql -U postgres -d etl_system
```

---

## 4. GC 与 JVM

```powershell
# 各服务 GC 日志位置不同（data-*-service/logs/gc.log），先汇总再逐份看停顿
Get-ChildItem -Recurse -Filter gc.log | ForEach-Object {
  $count = (Select-String -Path $_.FullName -Pattern 'Full GC' -ErrorAction SilentlyContinue).Count
  [pscustomobject]@{ File = $_.FullName; FullGC = $count }
} | Format-Table -AutoSize

# 对每个文件取最近 200 行，人工确认最大停顿
Get-ChildItem -Recurse -Filter gc.log | ForEach-Object {
  Write-Host "--- $($_.FullName) ---"
  Get-Content $_.FullName -Tail 200
}
```

```powershell
# 各服务堆使用（示例：task-management）
Invoke-RestMethod 'http://localhost:8082/actuator/metrics/jvm.memory.used?tag=area:heap'
```

---

## 5. Kafka lag

```powershell
# 容器名按 docker compose 实际名称调整
docker exec -it kafka kafka-consumer-groups.sh `
  --bootstrap-server localhost:9092 --describe --all-groups
```

记录每个 group 的 `LAG` 总和与最大值；基线窗口内至少采样 3 次（开始/高峰/结束）。

本次实测：运行中 t+15s 四组合计为 12,167–19,608；4 次任务全部结束后 `data-extraction-group`、`data-transformation-group`、`data-storage-group`、`task-management-group` 的 lag 均为 0。

---

## 6. 源库压力

PostgreSQL 源：

```sql
SELECT state, COUNT(*) FROM pg_stat_activity GROUP BY state;
SELECT pid, now() - query_start AS duration, query
FROM pg_stat_activity
WHERE state = 'active' AND now() - query_start > interval '5 seconds';
```

MySQL 源：

```sql
SHOW STATUS LIKE 'Threads_connected';
SHOW STATUS LIKE 'Threads_running';
SHOW PROCESSLIST;
```

记录采集窗口内的峰值连接数、CPU（由源库监控/云监控提供）、慢查询数。

---

## 7. 基线记录表（2026-10-19 实测，legacy 链路）

| 字段 | 值 |
|---|---|
| 采集时间 | 2026-10-19 18:30–18:34（+08:00） |
| 采集窗口 | 4 次连续任务，单任务 20,000 行；另 1 次冷启动样本单列 |
| Git commit | `c0aece6`（工作区含 Phase 0 未提交改动） |
| 数据量/表 | `etl_system.demo_baseline` → `demo_baseline_<taskId 前 8 位>`，20,000 行/次 |
| 任务数 | 4 个有效 warm 样本（另 1 个冷启动；1 个已执行污染样本与 7 个未执行任务被排除） |
| P50 / P95 / P99（秒，warm） | 45.57 / 49.73 / 50.31（最大 50.45）；冷启动 95.84 |
| 吞吐（行/秒，warm 平均） | 427.9（单次区间 396.4–440.9） |
| 失败率 | 0%（4/4 completed） |
| Full GC 次数 / 总停顿 / 最大停顿 | 未从 `gc.log` 采集（手工 `java -jar` 未开 `-Xlog:gc`）。Micrometer 四服务合计：每 run GC 22–27 次、停顿 0.17–3.06s；JVM 启动以来最大单次停顿 ≤0.08s |
| 源库峰值连接 / CPU / 慢查询 | 总连接峰值 73–75（`etl_system` 全部会话，含空闲与 psql）；CPU/慢查询未采集 |
| Kafka lag 峰值 | run 开始后 t+15s 合计 12,167–19,608（处理中积压）；任务结束后各 group lag=0 |
| 备注 | `etl.routing.new-worker=false`；Redis 中途重启导致旧链路 completion 依赖的计数器丢失，相关运行已排除 |

### 7.1 逐次明细（warm）

| run | taskId | 时长(s) | 目标行数 | 源库连接峰值 | t+15s lag | GC 停顿(s)/次数 | Redis TTL(s) |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | `0bdd6284-11f1-4a8d-91dc-10d7b33e276d` | 45.363 | 20,000 | 74 | 19,608 | 3.063 / 27 | 86,351 |
| 2 | `7d76799a-1ad6-4a10-ba5d-5b69f6fdade6` | 50.452 | 20,000 | 73 | 15,572 | 0.499 / 25 | 86,348 |
| 3 | `ac7bf53e-bf47-4131-aba2-59ca7e3bb79c` | 45.631 | 20,000 | 75 | 12,167 | 0.837 / 27 | 86,350 |
| 4 | `5da3d9c4-d182-47bb-90a7-a05eaba4f0a8` | 45.513 | 20,000 | 74 | 18,156 | 0.172 / 22 | 86,353 |

- GC 为 8082/8083/8084/8085 四服务 `jvm_gc_pause_seconds_*` 在单次 run 窗口内的合计增量。
- 冷启动样本 task `0336cccc-5727-45e1-9ecc-23d16a639800`：95.84s、20,000 行，含首次 JIT/连接建立开销，不与 warm 样本混合统计。
- 污染样本 task `34fd65f5-e7ab-4c36-95f7-08a76718cf29`：Redis 中断期间启动，目标表仅 19,230 行且 completion 无法判定，已排除；同时排除 7 个未执行任务（9–15）。
- 采样脚本与原始 JSON：`e2e-logs/run-baseline.ps1`、`e2e-logs/phase0-baseline.json`（`e2e-logs/` 不入库）。

---

## 8. 使用规则

- 这张表是 **Phase 1A 的对照基线**；Phase 1A 结束后用同一口径复测，不能换统计窗口或只挑成功任务。
- 如果采集时 `newWorker=true`，必须标注；该数据不能与 `newWorker=false` 基线直接比较。
- 没有基线的“P95 降 40%”不成立；Gate 1 只接受同口径的前后对比。
