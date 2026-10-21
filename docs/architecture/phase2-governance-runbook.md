# Phase 2 治理面运行手册（2026-10）

> 适用版本：Phase 2A–2F 落地后。覆盖审计、Outbox、夜间对账、目标表登记与清理、告警/面板。
> 设计依据：`orchestrator-design.md` §4.1/§5.1/§12.2；DDL 00–11 是唯一真源。

## 1. 数据模型与启动顺序

| 脚本 | 作用 |
|---|---|
| `db/schema/00-extensions.sql` | PostGIS 扩展（Phase 2A） |
| `02-create-tables.sql` | `data_source`/`etl_task`，含旧库 BIGINT 迁移与 `total_records/completed_records` 补列 |
| `05`–`07` | run / checkpoint / DLT |
| `08-etl-task-audit.sql` | 状态迁移审计（INV-9），无外键、保留 180 天 |
| `09-etl-outbox.sql` | 派工事务性 Outbox |
| `10-etl-reconciliation.sql` | 终态 run 对账结果（`UNIQUE run_id`） |
| `11-etl-target-table.sql` | 目标表登记/孤儿/清理状态 |

`task-management-service` 与 `data-source-service` 的 `ddl-auto` 已从 `update` 改为 **`validate`**。
新增实体字段时必须先改 `db/schema`，否则服务启动即失败；IT 的 `application-it.yml` 同样为 `validate`。

## 2. 审计（2B / INV-9）

- 唯一写入口：`OrchestratorService.transition()` 成功后调用 `AuditWriter`，与状态写同事务；失败则整个迁移回滚。
- 新建 run 也记一条 `(none) -> CREATED`；CAS 失败/非法迁移/guard 拦截不写。
- 查询：`GET /api/tasks/{taskId}/audit?page=0&size=50`（按 id 倒序，size 上限 200）。
- 保留：`etl.audit.retention-days`（默认 180，`<=0` 关闭），每日 03:00 清理。
- 指标：`etl_audit_write_failures_total`、`etl_audit_cleanup_deleted_total`。

快速核对：

```sql
-- 每个 run 的审计条数应与“已落地迁移次数”一致；终态 run 至少 2 条（CREATED + 后续）
SELECT task_id, run_id, COUNT(*) FROM etl_task_audit GROUP BY task_id, run_id ORDER BY 3 DESC;
-- 不应出现“状态已变但无审计”的 run（允许旧链路历史数据）
SELECT r.run_id FROM etl_task_run r
WHERE NOT EXISTS (SELECT 1 FROM etl_task_audit a WHERE a.run_id = r.run_id);
```

## 3. Outbox（2C）

- 开关：`etl.outbox.enabled`（默认 **false**，直投 Kafka）。
- `true` 时 `DispatchService.dispatchOne` 顺序为：`markAssigned` CAS 成功 → 插入 `etl_outbox` → 提交；
  发布失败抛异常回滚 CAS，分片保持 PENDING。poller 异步投递，at-least-once，worker 以 `idempotency_key` 去重。
- 回滚：改回 `false` 并重启 task-management；已入箱行仍会被 poller 继续投递（避免丢消息）。
- 观测：`etl_outbox_pending`、`etl_outbox_oldest_pending_age_seconds`、`etl_outbox_sent_total`、
  `etl_outbox_send_failures_total`、`etl_outbox_cleaned_total`。
- 手工排查：

```sql
SELECT id, topic, message_key, attempt_no, next_attempt_at, last_error
FROM etl_outbox WHERE sent_at IS NULL ORDER BY id LIMIT 50;
```

## 4. 夜间对账（2D）

- 定时：`etl.reconciliation.cron`（默认 02:30），扫描近 `lookback-hours`（48h）内
  `SUCCEEDED/FAILED/CANCELLED` 的 run；每个 run 一条结果，已存在则跳过。
- 口径：
  - SUCCEEDED：`source_rows - rows_filtered - rows_failed = target_rows`；
  - 其它终态：`target_rows = checkpoint SUM(rows_out)`；
  - 共同：`target_rows = COUNT(DISTINCT idempotency_key)`，且四段平衡
    `rows_in = rows_out + rows_filtered + rows_failed + rows_deduped`。
- API：`GET /api/reconciliation/{runId}`、`GET /api/reconciliation?taskId=...`、
  `POST /api/reconciliation/{runId}`（强制重算）。
- 指标：`etl_reconciliation_evaluations_total{result="PASS|MISMATCH|ERROR"}`。
- 对账失败不自动改任务状态；先查 `details`，确认数据/表名/数据源后再处理。

## 5. 目标表登记与清理（2E）

- 创建任务时登记 `etl_target_table`；同一目标库表名只能归一个 task，冲突返回 **409**。
- `PUT /api/tasks/{id}` 冻结 `dataSourceId/sourceTableName/targetTableName`（修改返回 **400**），
  仅描述等非定位字段可改。
- 每日 04:00 只把“task 已删除，或终态超过 `orphan-retention-days`”的表标为 `ORPHANED`，**不删表**。
- 清理 API：
  ```bash
  # 1) 先 dry-run，确认候选
  curl -X POST "http://localhost:8080/api/admin/target-tables/cleanup?dryRun=true&retentionDays=7"
  # 2) 再真正删除 ORPHANED 表（不可逆）
  curl -X POST "http://localhost:8080/api/admin/target-tables/cleanup?dryRun=false&retentionDays=7"
  ```
- 列表：`GET /api/admin/target-tables?state=ACTIVE|ORPHANED|PURGED`。
- 指标：`etl_target_table_count{state=...}`。
- 清理只接受安全标识符；`DROP TABLE IF EXISTS` 失败会保留 `ORPHANED` 并在返回的 `details` 中列出。

## 6. 告警与面板

- `monitoring/prometheus/rules/alerts.yml` 新增 `etl-phase2-governance-alerts`：
  审计写失败、Outbox 卡住/积压/失败、对账 MISMATCH/ERROR、孤儿目标表。
- `monitoring/grafana/dashboards/etl_phase2_governance.json` 提供审计、Outbox、对账、目标表面板。
- 全量指标仍从各服务 `/actuator/prometheus` 抓取（`monitoring/prometheus/prometheus.yml`）。

## 7. 验证命令

```bash
# 单元测试（默认跳过 @Tag("it")）
mvn test

# 定向集成测试（需 docker compose up -d postgres redis kafka；MySQL 回归另需 --profile mysql）
mvn -pl task-management-service -am test -Pit \
  "-Dtest=SchemaValidationIT,OrchestratorStateMachineIT,OutboxDispatchIT,ReconciliationIT,TargetTableCleanupIT" \
  "-Dsurefire.failIfNoSpecifiedTests=false"

# MySQL / PostGIS 真库回归
docker compose --profile mysql up -d mysql
mvn -pl task-management-service -am test -Pit \
  "-Dtest=MySqlToPostgresSinkIT,PostgisSinkIT" "-Dsurefire.failIfNoSpecifiedTests=false"
```

检查 IT 输出中的 `Skipped: 0`；基础设施缺失时 IT 会跳过而不是失败。

## 8. 边界与注意事项

- 审计/对账/目标表登记表都**不建 task 外键**，任务删除后仍保留审计与清理追溯。
- Outbox 只覆盖派工消息；`EtlTaskTransitioned` 没有 Kafka 消费者，不做 outbox。
- 对账只读外部库，不修改源/目标数据；`MISMATCH` 不会自动重试或重跑任务。
- 目标表清理是破坏性操作，必须先 dry-run；`PURGED` 行保留表名与时间。
- Phase 2 未引入 Mongo/File 等新 connector；MySQL 仅作为回归目标库。
