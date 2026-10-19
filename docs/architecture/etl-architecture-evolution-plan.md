# ETL 架构演进方案：从四段 Kafka 流水线到"编排器 + In-Process Worker"

> 产出方式：`system-modeler`（结构与边界）+ `flow-visualizer`（端到端流程）+ `risk-quality-reviewer`（问题清单）+ `evolution-planner`（目标架构与迁移路线）。
> 视图状态：现状为 **current-state**（代码/配置直接证据），目标为 **target/proposed**（显式标注）。
> 配套现状图：`architecture-container-view.dot`、`etl-task-lifecycle-flow.dot`；目标态图：`target-container-view.dot`、`target-lifecycle-states.dot`、`target-dataflow.dot`。
> 补充件（H~M 风险 + 业界选型）：`risk-addendum-and-tech-selection.md`、`risk-tech-selection-matrix.dot`。
> 详细设计：`orchestrator-design.md`（状态机 + 守护规则 + outbox + sweeper，已补齐 §3/§4 的缺口与不一致）。
> 渲染：`dot -Tsvg xxx.dot -o xxx.svg`。

## 摘要

针对当前 data-etl 的 4 阶段 Kafka 流水线，把 E/T/L 合并为单一 **in-process worker**，引入轻量 **编排器 + 显式状态机 + checkpoint/事务日志**，Kafka 退居任务分发与流式通道；分四阶段渐进迁移，覆盖 MySQL/PG、NoSQL、JSON/CSV/GeoJSON、IoT 与批/流两种模式，直击"链路长、中间失败难定位、是否结束难判断、失败恢复难"四大痛点。规模假设：单团队、单表 ≤ 30M 行、并发任务 ≤ 20，避免为微服务而微服务。

---

## 1. 当前架构与问题清单

### 1.1 现有端到端流程（current-state，高置信）

见 `etl-task-lifecycle-flow.dot`、`architecture-container-view.dot`。

- **控制流**：`POST /api/tasks` → `etl_task.status='new'` → `TaskScheduler` 每 30s 扫描（Redisson 锁 `task:scheduler:lock`）→ Feign 拿数据源 → 反射建目标表（`TaskScheduler.generateCreateTableSql`）→ 发 Kafka `task-request`（key=taskId）→ `status='running'`。
- **数据流**（每行一条 Kafka 消息，taskId 作 key）：`task-request` → `data-extraction-service`（`MIN/MAX/COUNT` + `WHERE id>=? LIMIT 1000` 顺序游标，逐行 `send` 到 `raw-data`）→ `data-transformation-service`（batch 监听 + CompletableFuture 并行、`send().get()` 同步发送）→ `data-storage-service`（每行一次 Feign 取表名 → 拼 INSERT → 每行 `HINCRBY completedRecords`；`>=totalRecords` 时发 `task-completed`）。
- **回写**：`TaskCompletedListener` 置 `completed`；`data-extraction-service/.../KafkaConfig.java:73-98` 在重试耗尽后发 `task-failed`，`EtlTaskService.markFailed` 回滚非终态。
- **状态字面量**（`EtlTask.status` 为 `String`）：`new/processing/running/completed/failed`——无枚举、无终态保护、无 attempt 概念。

### 1.2 问题清单（按痛点归组，H~M 详见 `risk-addendum-and-tech-selection.md`）

| # | 痛点 | 严重度 | 现象 / 证据 |
|---|---|---|---|
| A1 | 链路长 | P0 | 一行数据跨 4 服务 + 3 次 Kafka + 2 次 Feign |
| A2 | 链路长 | P0 | 单行消息 + 单行 Feign + 单行 INSERT → task-mgmt Full GC 风暴 52 次/均值 94ms（`performance-analysis.md §2`） |
| A3 | 链路长 | P1 | 无分片并行，README 承诺的"主键范围分片 + CompletableFuture"现代码不存在 |
| B1 | 中间失败难定位 | P0 | traceId 未贯通；日志无 taskId/shardId MDC；业务埋点未落地 |
| B2 | 中间失败难定位 | P0 | 5 个 topic 无 DLT，仅 extraction 侧有 `FixedBackOff(1000,3)` |
| B3 | 中间失败难定位 | P1 | 目标表无主键/索引（`generateCreateTableSql`） |
| C1 | 结束难判断 | P0 | 完成判定依赖 Redis Hash + `task-completed`，键无 TTL、丢失即永卡 running |
| C2 | 结束难判断 | P0 | `completedRecords >= totalRecords` 中 `totalRecords` 来自 `COUNT(*)` 快照，源表并发写即两边不等 |
| C3 | 结束难判断 | P2 | 无 attempt/run 概念，`earliest` 重放与"是否已完成"混淆 |
| D1 | 失败恢复难 | P0 | 无 checkpoint 表；重试=从头再抽；无"仅重跑失败分片" |
| D2 | 失败恢复难 | P0 | 目标表无 PK → 无法幂等；去重靠 Redis 24h TTL |
| D3 | 失败恢复难 | P1 | 仅 extraction 有 `task-failed`；transformation/storage 失败仅吞异常 |
| D4 | 失败恢复难 | P1 | 无人工干预入口；无 `CANCELLED`/`PARTIAL_SUCCESS` |
| E1 | 接入面窄 | P0 | 仅 JDBC；Mongo/文件(JSON/CSV/GeoJSON)/IoT 全缺位 |
| E2 | 接入面窄 | P0 | 无流式管道；IoT/CDC 只能伪装成 batch |
| F1 | 微服务过度 | P1 | E/T/L 三服务技术栈/团队/扩缩需求一致，拆开只增运维面 |
| F2 | 微服务过度 | P2 | 转换默认规则仅"数值保留 2 位小数"，不需独立部署 |

> 另有 H（数据正确性）、I（并行度天花板，taskId 分区键杀死单任务并行）、J（安全/凭据明文）、K（单点/运维）、L（契约）、M（治理）六类补充问题，见补充件。

---

## 2. 推荐总体架构（target-state）

配套图：`target-container-view.dot`。

### 2.1 服务合并/保留决策

| 现状服务 | 决策 | 目标形态 | 依据 |
|---|---|---|---|
| registry / config-server / gateway / user-service | 保留 | 不动，仅补 tracing propagation 与限流阈值 | 边界清晰，替换成本高 |
| data-source-service | 保留（瘦身为纯元数据 CRUD） | 增加 `connectorType`/`connectionSchema` 支持非 JDBC 源 | 变更频率与主链路解耦 |
| task-management-service | 升级为 `etl-orchestrator` | 显式状态机 + `etl_task_run`/`etl_task_checkpoint`/`etl_task_dlt`；暴露 submit/cancel/retry/resume；仅编排不处理数据 | 直接解 B/C/D |
| data-extraction + data-transformation + data-storage | 合并为一个 `etl-worker`（同 jar，profile 分批/流） | E→T→L 全进程内串成 pipeline，Kafka 仅分发与回报，不穿越数据面 | 解 A/B/C/D/F；无独立伸缩诉求 |
| — | 新增 `etl-stream-runtime`（`etl-worker` 的 stream profile） | 常驻消费者处理 IoT/CDC/文件监听 | 解 E2 |
| — | 新增 `connector-spi` 模块（jar，非服务） | 统一 Source/Sink 接口，Jdbc/Mongo/File/IoT 各一实现 | 解 E1，避免"每种源一个新微服务" |

关键取舍：**不引入 Spring Batch、不引入 SeaTunnel/DolphinScheduler/Airflow**——数据量 2~3 千万、单团队、需精细几何/PostGIS/GeoJSON 处理，用"状态机 + checkpoint 表"自实现编排语义更省维护成本。若后续 > 500 并发或需跨节点 DAG，再评估切 Airflow/DolphinScheduler，接口预留 `OrchestratorEngine` 抽象即可。（业界选型对照见 `risk-addendum-and-tech-selection.md §2`。）

### 2.2 控制面 / 数据面 / 流面

- **控制面**：gateway → orchestrator（原 task-mgmt :8082）；orchestrator ↔ data-source（元数据）；orchestrator ↔ PostgreSQL（`etl_task`/`etl_task_run`/`etl_task_checkpoint`/`etl_task_dlt`）；orchestrator 生产 `etl.task.dispatch.batch`、消费 `progress`/`results`/`dlt`。
- **数据面**：`etl-worker`（profile=batch，可扩 1~N）从 `dispatch.batch` 拉活，进程内 `SourceReader → TransformerChain → SinkWriter`；每 shard 提交 checkpoint；批结束发 `results`。
- **流面**：`etl-worker`（profile=stream）从 `source.iot.*`/`source.file.*` 拉，走同一 SPI 到 sink，发心跳/指标到 orchestrator。
- **Redis 收缩**：只保留分布式锁 + 热点缓存（源表元数据、target-table-name），不再作完成判定唯一真源。

---

## 3. 任务全生命周期状态模型

> 本节为**概要**。逐状态的语义/SLA/超时动作、32 条合法迁移边、行数对账推导、sweeper 判定树与决策记录，见 `orchestrator-design.md`（§3–§8）；`target-lifecycle-states.dot` 已按该文档修订并重新渲染 SVG。

配套图：`target-lifecycle-states.dot`。

### 3.1 状态集合（`EtlTaskStatus` 枚举，取代字面量）

`CREATED`, `VALIDATED`, `QUEUED`, `DISPATCHED`, `RUNNING`, `PARTIAL_SUCCESS`, `RETRYING`, `NEEDS_ATTENTION`, `SUCCEEDED`, `FAILED`, `CANCELLED`, `SUPERSEDED`

- **`etl_task`**：任务定义 + "最新一次运行汇总视图"。
- **`etl_task_run`**（target 新表）：每次执行一条，`runId, taskId, attemptNo, status, startedAt, finishedAt, stats, errorSummary`；`etl_task.status` 派生自最新 run。
- **`etl_task_checkpoint`**（target 新表）：`(taskId, runId, shardId)` 主键，`lastCursor/lastOffset/lastPk, rowsIn, rowsOut, status, updatedAt`；断点续传唯一真源。

### 3.2 迁移与守护规则（集中在 `OrchestratorService.transition(taskId, from, to, reason)`，禁止散点 `task.setStatus(...)`）

1. 终态（SUCCEEDED/FAILED/CANCELLED/SUPERSEDED）不可回退；NEEDS_ATTENTION 仅经 `/retry` 或 `/resume` API 迁出。
2. RUNNING → PARTIAL_SUCCESS 要求 checkpoint 存在 FAILED 分片且其余分片 SUCCEEDED。
3. DISPATCHED → RUNNING 由 worker 首次心跳确认；60s 无心跳 → 回退 QUEUED 且 `attemptNo+1`，避免"派了没 worker 领"死锁。
4. RUNNING → SUCCEEDED 必须"所有 shard checkpoint=SUCCEEDED 且 rowsOut 汇总 == rowsIn 汇总 - rowsDropped"——以分片游标实读行数取代 `COUNT(*)` 快照，修复 C2。
5. CANCELLED：orchestrator 发 `etl.task.commands`，worker 完成当前 chunk 后停并落 checkpoint；支持后续 `/resume`。

---

## 4. 失败处置机制

| 机制 | 方案 | 关键点 |
|---|---|---|
| 重试策略 | 三段式：瞬态（网络/锁/超时）分片内指数退避+抖动 ≤3；持续性任务级 `attemptNo+1` 退避 5min/15min/1h ≤3；永久性（schema 不匹配/脏数据）立即 DLT 不重试 | `ErrorClassifier`（Retryable/Transient/Fatal）替换 `DataStorageService:150-164` 盲重试 |
| 补偿/人工干预 | `POST /api/tasks/{taskId}/retry?shards=failed&from=checkpoint`、`/cancel`、`/mark-succeeded-with-data-loss`；操作写 `etl_task_audit` | 解 D4 |
| 幂等键 | `idempotencyKey = sha256(taskId+runId+sourceUri+shardId+rowPk)`；目标表加 `idempotency_key UNIQUE`/PK；`INSERT ... ON CONFLICT DO UPDATE/NOTHING` | `generateCreateTableSql` 必须补 PK/唯一索引 |
| 去重 | 主：DB 唯一约束（不依赖 Redis）；副：Redis 布隆过滤器热路径预筛，DB 兜底 | 解 D2、兑现 README |
| 断点续传 | worker 每 chunk（默认 5k 行）事务提交：① 目标表 batchUpsert ② checkpoint upsert ③ outbox 插入；Kafka 由 poller 从 outbox 投递（**Transactional Outbox**） | 避免"数据已提交但 progress 丢失"；解 C1/C2/D1 |
| 毒消息 DLT | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` → `etl.task.dlt` + `etl_task_dlt`（rawPayload/errorClass）；DLT 非空即 NEEDS_ATTENTION | 三处 KafkaConfig 升级统一模板 |
| 失败可见性 | `run.errorSummary` 存首个致命异常 stack 前 2KB + shard 列表；`GET /api/tasks/{taskId}/runs/{runId}/failures` | 解 B1 |

---

## 5. 数据流设计

配套图：`target-dataflow.dot`。

### 5.1 Kafka topic 重规划（target）

| Topic | 分区 | 保留 | 生产 | 消费 | 用途 |
|---|---|---|---|---|---|
| `etl.task.commands` | 12 | 7d | orchestrator | worker/stream | 取消/暂停/恢复，key=taskId |
| `etl.task.dispatch.batch` | 24 | 7d | orchestrator | worker | 分片级派工，一 shard 一消息 |
| `etl.task.dispatch.stream` | 按源 | 7d | — | stream（静态分配） | 流任务注册（可选直连源 topic） |
| `etl.task.progress` | 12 | 3d | worker | orchestrator | 心跳/进度（每 shard ≤1/5s） |
| `etl.task.results` | 12 | 30d | worker | orchestrator | run 终态与统计 |
| `etl.task.dlt` | 6 | 30d | worker | orchestrator+运维 | 毒消息 |
| `source.iot.raw.*` | 24+ | 按业务 | 外部 | stream | IoT 直连（MQTT bridge） |
| `source.file.arrival` | 6 | 7d | 文件监听 | orchestrator | 对象存储事件（MinIO/S3 webhook） |

**删除**：`raw-data`、`transformed-data`、`task-completed`、`task-failed`——语义并入 `dispatch/progress/results`。

### 5.2 批处理分片策略（worker profile=batch）

- JDBC：按 `MIN(id)/MAX(id)` 分 N 段，一段一 shard 消息；无 PK 表用 `TABLESAMPLE`/`ctid`；流式游标 fetchSize（MySQL `Integer.MIN_VALUE` / PG 5000）。**分片消息只带 (taskId, runId, shardId, pkFrom, pkTo)，不传整行**。
- Mongo：`_id` 范围 + `$bucketAggregate` 分片。
- 文件/对象：CSV 行区间 + gzip offset index；JSON 顶层数组切段/JSON stream；GeoJSON 按 Feature 分片（≤200MB 内存 FeatureIterator，>200MB 落临时 spool）。
- 默认 chunk 5,000 行，上限 20,000（PostGIS 场景降 2,000 避 `ST_GeomFromText` CPU 尖峰）。

### 5.3 流处理低延迟通道（worker profile=stream）

- 常驻消费 `source.iot.raw.*`，同 SPI：`StreamReader → Decode/Normalize → SinkBuffer → SinkWriter`。
- 微批：`linger 200ms` 或 `buffer 500` 触发 flush；p95 端到端 < 3s。
- 按 source 分消费者组水平扩缩；原生 `KafkaConsumer` + `ConsumerRebalanceListener` 管 shard 生命周期；与批任务共用 checkpoint 表（streamId 代 taskId）。

### 5.4 背压与限流

- Worker 拉模型：`concurrency` + `maxPollRecords` + 本地 `Semaphore(activeShards=cpu*2)`；获取失败 → `pause()` 对应 partition。
- Producer：`linger.ms=10, batch.size=256KB, compression.type=zstd`（现 extraction 已有，扩至所有生产者）。
- Sink：`autoCommit=false` + `executeBatch()` 每 5k 提交；PG 目标可用 `COPY`（`CopyManager.copyIn`）替代 INSERT。
- 全局：orchestrator 令牌桶限制"同时 RUNNING 分片数"，护住源库。

---

## 6. 可观测性与监控告警

### 6.1 落地清单

- **追踪**：Micrometer Tracing / Spring Cloud Sleuth + OTLP → Jaeger/Tempo；KafkaHeaders 传播 traceId；一次 ETL = 一个 trace，跨 orchestrator/worker/PG/Redis。
- **指标**（Micrometer，带 taskId/sourceType/pipelineType 标签）：`etl.task.dispatch.count{status}`、`etl.task.duration`(P50/95/99)、`etl.shard.duration{stage}`、`etl.records.{in|out|dropped|deduped}`、`etl.retry.count{errorClass}`、`etl.checkpoint.lag`、`etl.dlt.size`；复用 `kafka_consumer_..._records_lag`。
- **日志**：JSON encoder + MDC `traceId/spanId/taskId/runId/shardId/attemptNo`；ERROR=触发 DLT/NEEDS_ATTENTION，WARN=分片内重试，INFO=shard 提交。
- **进度**：唯一真源 = `etl_task_checkpoint`（orchestrator 直查）；Redis 仅活跃分片热缓存 + 展示层短 TTL；UI `GET /api/tasks/{taskId}/progress`。

### 6.2 Grafana 面板（`monitoring/grafana/dashboards/`）

`etl_task_overview.json`、`etl_pipeline_hotpath.json`、`jvm_gc.json`（治 A2）、`postgres_health.json`（慢查询 Top10）。

### 6.3 告警规则（`monitoring/prometheus/rules/alerts.yml`）

| 级别 | 规则 | 阈值 |
|---|---|---|
| critical | EtlTaskNeedsAttention | status==NEEDS_ATTENTION 持续 >5min |
| critical | EtlDltGrowing | `increase(etl_dlt_size_total[10m]) > 20` |
| warning | EtlTaskStalled | `time()-last_progress_ts > 15*shard_expected` |
| warning | EtlConsumerLagHigh | lag>5000 持续 10min |
| warning | JvmFullGcStorm | major GC rate >0.1 |
| info | EtlTaskSlowRegression | P95 相对 7d 基线 +50% |

### 6.4 PostgreSQL

`docker-compose.yml` 的 `postgres` 加 `-c log_min_duration_statement=100 -c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.track=all`，修复 `performance-analysis.md §1` 空缺。

---

## 7. 渐进迁移路线与风险规避

### 7.1 阶段划分（每阶段可独立回滚）

- **Phase 0 · 观测先行（1~2 周，零业务改动）**：落地 §6.1 追踪/指标/JSON 日志；PG 慢查询参数；task-mgmt 堆 512M→1G；storage `getTargetTableNameFromTaskService` 加 Caffeine 缓存；`storeBatchDataToDatabase` 改真批。回滚=配置/依赖级。
  - ✅ **已实施（2026-10，全 10 模块 `mvn compile` BUILD SUCCESS）**：task-mgmt 堆 1G；storage 目标表名 Caffeine 缓存 + batch listener 真批 + 单事务切片提交 + 批内去重 + MDC(taskId)；Sleuth 接入 task-mgmt/extraction/transformation/storage 4 服务并加 traceId/spanId/taskId 日志格式；`EtlMetrics`(etl.records.{in,out,dropped,deduped}/etl.load.duration/etl.insert.retry)；`docker-compose.yml` 开 `pg_stat_statements`+慢查询；新增 Grafana `etl_pipeline_hotpath.json`；`alerts.yml` 加 5 条流水线/JVM 告警。详见 `performance-analysis.md §6.5`。
- **Phase 1 · Worker 骨架 + 状态机（3~6 周）**：新建 `etl-worker`（profile batch/stream）+ `SourceConnector`/`SinkWriter` SPI + Jdbc 实现；引入 `etl_task_run`/`etl_task_checkpoint` + `EtlTaskStatus`；task-mgmt 升级为 orchestrator，写 `dispatch.batch`+`progress`+`results`，旧 topic 保留按 `dataSourceId` 灰度；影子运行新旧链路比对。**回滚**：`etl.routing.newWorker=false` 全退旧链路，旧 topic 未动。
  - 🔶 **状态机内核已落地（2026-10，分支 `etl-orchestrator`，全 10 模块 `mvn compile` + 内核 7 项单测 BUILD SUCCESS）**：`EtlTaskStatus`(12)/`EtlShardStatus`(6)/`ErrorClass` 枚举；DDL `db/schema/05-etl-task-run.sql`、`06-etl-task-checkpoint.sql`（含 INV 部分唯一索引 + CHECK）；`orchestrator/` 包内 `EtlTaskRun`/`EtlTaskCheckpoint` 实体与 Repository（`casStatus` 乐观并发、`aggregate` 收敛聚合、`refreshDerivedStatus` INV-6）；`AllowedTransitions`（迁移表，穷举单测锁定 **恰 32 条合法边**、112 条非法）+ `GuardChain`（G-2/G-3/G-4/G-5）+ `OrchestratorService.transition()`（非法抛异常、CAS 幂等、派生刷新、AFTER_COMMIT 事件）+ `DomainEventPublisher`(Phase 1 语义) + `OrchestratorEngine` 预留抽象 + `OrchestratorMetrics`。
  - ⏳ **Phase 1 待办（后续切片）**：四个 sweeper、`DispatchService`+令牌桶、`Progress/Results/Dlt` 监听与新 topic 接线、旧监听器适配层 + 封堵 `PUT /api/tasks/{id}` 的 status 入参、`etl-connector-spi`/`etl-worker` 模块骨架、影子运行。旧链路 `EtlTaskService/TaskScheduler` 的字面量 `setStatus` 暂保留（不破坏现网），到接线切片统一收口（§11.3 grep 门禁）。
- **Phase 2 · 多源 SPI + 失败处置完备（7~9 周）**：`MongoConnector`/`FileConnector`(JSON/CSV/GeoJSON)；`generateCreateTableSql` 强制 PK + `idempotency_key UNIQUE`；DLT + ErrorClassifier + Outbox；`/retry?shards=failed`、`/cancel`；非 JDBC 全走新链路，JDBC 25%→50%→100% 切流。
- **Phase 3 · 流式通道（10~12 周）**：上线 stream-runtime；接 IoT（`source.iot.raw.*`）、文件监听（MinIO webhook→`source.file.arrival`）；微批 sink + offset checkpoint；UI 加 stream 列表与 lag 面板。
- **Phase 4 · 旧链路下线（13~14 周）**：连续 7 天旧 topic 无生产且 lag=0 → 删除三服务、从父 `pom.xml <modules>` 移除、删旧 topic、同步 `docs/` 与 `README.md`。

### 7.2 风险登记

| 风险 | 影响 | 缓解 |
|---|---|---|
| worker 单进程 E-T-L 爆炸半径大 | 一错规则中断整批 | Bulkhead `activeShards` 上限；shard 级重试；DLT 隔离毒 shard |
| 20~30M 行 + PostGIS → OOM | worker 频繁 GC | fetchSize 流式 + chunk≤5k + 堆 2G + `G1MaxNewSizePercent=50`；大 GeoJSON spool 临时目录 |
| 新旧 topic 并存消息分裂 | UI/回滚不一致 | 双消费组写同一 `etl_task_run`；orchestrator 幂等 `(taskId,runId)` |
| 状态机触及全部读写路径 | 遗漏致状态漂移 | 移除全部 `setStatus("literal")`，grep 校验只允许 `transition(...)` |
| 无 PK 历史目标表 | 加 PK 唯一性冲突 | Phase 2 迁移脚本：加 `idempotency_key` 回填去重后 `ALTER ADD PRIMARY KEY` |
| Redis 完成判定退位 | 旧展示层断链 | UI 改查 checkpoint；Redis Hash 保留 24h 加速缓存 |
| 团队学习状态机/outbox | 交付延期 | 手写状态表 + `@TransactionalEventListener`，不引 Spring StateMachine；配 `orchestrator-design.md`+runbook |
| Phase 4 删服务后回滚窗口关闭 | 严重问题需重部署 | 保留 3 个月发布产物与 config；`git tag pre-etl-merge-v1` |

### 7.3 度量与验收

- A：Phase 1 后端到端跳数 7→3；P95 duration 相对基线降 ≥40%。
- B：任一 NEEDS_ATTENTION 任务 30s 内拿到 stack + shard 列表 + DLT 原文。
- C：`etl_task.status` 与 checkpoint 汇总一致性 100%（nightly 校验）。
- D：Phase 2 后从 checkpoint 恢复重跑成功率 ≥95%；单 shard 重试不影响其他。
- E：Phase 3 后支持 4 类源 + 流批双模式，`data_source.type` 至少含 `mysql|postgres|mongo|file_json|file_csv|file_geojson|kafka_iot`。

---

## 8. 落地需产出的仓库文件（Phase 0/1 首批）

- 目标态图：`target-container-view.dot`、`target-lifecycle-states.dot`、`target-dataflow.dot`（本目录，已随本方案创建）。
- 设计文档：`orchestrator-design.md`（状态机 + outbox + SPI 契约）—— ✅ **已产出**（含 §3/§4 规则与判定的补齐清单与 25 条决策记录）；`connector-spi.md`（Source/Sink 接口签名）、`migration-runbook.md`（每阶段开关、验证 SQL、回滚指令）——随 Phase 0/1 实施编写。
- DDL：`db/schema/05-etl-task-run.sql`、`06-etl-task-checkpoint.sql`、`07-etl-task-dlt.sql`、`08-etl-task-audit.sql`。
- 代码：新增 `etl-connector-spi/`、`etl-worker/`；`task-management-service` 内 `orchestrator/` 包与 `EtlTaskStatus` 枚举。
- 监控：`etl_pipeline_hotpath.json`、`postgres_health.json`；`alerts.yml` 扩充 §6.3 六条。

---

## 9. 假设与待确认

1. 数据规模：单任务 ≤ 30M 行、并发 ≤ 20、节点 4C8G 起；若并发 > 100/多租户需引入 per-task 派生模式，预留 `DispatchStrategy` 抽象。
2. 团队 ≤ 5 人、无独立 SRE，可观测优先复用现 Prometheus/Grafana 栈。
3. 保留 Kafka 作流式源与事件总线；**批处理数据面不再穿越 Kafka**，仅控制面（dispatch/progress/results/dlt）。
4. 不引入编排框架；自研状态机 + checkpoint。后续出现跨任务 DAG/多集群调度再评估并接入 `OrchestratorEngine` 抽象（业界对照见 `risk-addendum-and-tech-selection.md`）。
5. `user-service` 与 gateway JWT 保留；orchestrator 的 `/retry`、`/cancel` 走同一 RBAC。
