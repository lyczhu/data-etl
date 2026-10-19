# ETL 架构演进计划 v2：先修数据面，再收控制面

> 版本：v2.0（2026-10-19）
> 状态：**有条件推进**。`etl.routing.new-worker` 保持 `false`，直到 Gate 1（数据面正确性）通过。
> 团队假设：10 人（控制面 2、数据面 3、平台/安全 2、测试 2、产品/运维 1）；关键路径不因加人而压缩。
> 依据：`etl-architecture-review-2026-10-19.md`（下称“评审报告”）、当前 `task-management-service`/`etl-worker`/`etl-connector-spi`/`db/schema`/`config-repo` 的实际实现。
> 文档关系：`orchestrator-design.md` 仍是状态机与迁移边的权威契约；本计划只调整**阶段顺序、范围、验收门槛**，不推翻已实现的 12 态/32 边。v1 内容见 git 历史。
> 视图状态：现状为 current-state（代码/配置直接证据）；目标为 target/proposed（显式标注）。

---

## 0. 执行摘要

### 0.1 结论

- **继续架构方向**：E/T/L 合并为 in-process worker、checkpoint 作为真源、单一 `transition()` 入口、DB 约束优先，这些方向保留。
- **不按 v1 直接进入生产灰度**：评审报告中的 P0-1～P0-8 未关闭前，`newWorker=true` 只允许在实验室/测试环境使用。
- **先修数据面，再收控制面**：Phase 1 的第一优先级不是继续扩状态机，而是“全量派工 + 原子 checkpoint + 可续跑 + 幂等 + 重试/DLT + 安全”。
- **Phase 1A 前 2 周内做一次技术路线决策**：自研 worker 的可靠性部分能否在可控成本内达到 Gate 1；否则用 Spring Batch 承担 chunk/restart/skip/retry/partition，或用 Temporal 承担退避/超时/取消/历史。

### 0.2 v1 的三个最致命阻断

1. **派工不完整**：`DispatchService` 只派首批 `permitsPerRun=8` 个分片，run 进入 `DISPATCHED/RUNNING` 后不再补派；30M 行/5000～6000 分片只会跑前 8 片。
2. **断点不可续**：`last_cursor` 不按 chunk 写入，worker 不读 `resumeCursor`；数据写入与 checkpoint 不在同一事务，重试会重复写且 G-4 可能看不出来。
3. **失败不可处置**：没有 `ErrorClassifier`、没有自动 T12 重试、worker 不生产 DLT；`rowsDropped` 混用，坏行/丢数仍可能被判 `SUCCEEDED`。

### 0.3 v2 的七个变化

| # | 变化 | 原因 |
|---|---|---|
| 1 | Phase 1 范围缩为“数据面正确性 gate” | 控制面已足够复杂，数据面未达标 |
| 2 | 目标表 PK/幂等/upsert 提前到 Phase 1 | 没有 PK 的重试必然重复 |
| 3 | 明确提交边界（同库同事务/跨库幂等重放），chunk 级游标 | “断点续传唯一真源”必须可读可写，且不能假设跨库 2PC |
| 4 | 分片数按并行度定，chunk 只做事务单元 | 30M/5k=6000 分片会超过上限并放大控制面 |
| 5 | 全局许可改为 DB 计数/分布式信号量 | JVM `Semaphore` 不是全局、不抗重启 |
| 6 | 派工 key 改为 `runId+shardId`，worker 配置并发 | 修复 I1“taskId 分区键杀死单任务并行” |
| 7 | 灰度按 dataSource，shadow compare 真正落地 | 全局布尔开关无法灰度/回滚 |

### 0.4 不改变的内容

- 12 个 run 状态、32 条合法迁移边、`OrchestratorService.transition()` 单一入口。
- `etl_task_run` / `etl_task_checkpoint` 三层模型与现有 DDL 05/06。
- 4 个 sweeper 的骨架（但需要补收敛检查、lease/fencing、DB 许可）。
- Phase 0 已完成的堆内存、storage 真批、Caffeine、Sleuth、指标与慢查询配置。
- `etl.routing.new-worker` 作为总开关；新增 dataSource 白名单作为灰度细粒度。

---

## 1. 现状基线（current-state）

### 1.1 当前运行模式

- 生产默认仍走旧链路：`EtlTaskService` 建 `status='new'` → `TaskScheduler` 扫描 → 发 `task-request` → 旧 E/T/L 三服务 → Redis 计数 → `task-completed/task-failed` 回写。
- 新链路已实现但默认休眠：`etl.routing.new-worker=false`（`config-repo/task-management-service.yml`）。
- 新链路组件：`EtlTaskStatus`、`AllowedTransitions`、`GuardChain`、`OrchestratorService`、4 个 sweeper、`DispatchService`、`ProgressListener/ResultsListener/DltListener`、`RunValidator`、`etl-worker`、`etl-connector-spi`。

### 1.2 已实现 / 未实现

| 能力 | 状态 | 证据 |
|---|---|---|
| 12 态 + 32 边 + CAS + 派生投影 | 已实现 | `AllowedTransitions`、`OrchestratorService`、`EtlTaskRunRepository` |
| `etl_task_run` / `etl_task_checkpoint` DDL | 已实现 | `db/schema/05`、`06` |
| 4 个 sweeper + 退避策略 | 已实现（有缺口） | `orchestrator/scheduler/*`、`policy/*` |
| 入口链 submit→validate→plan→queue | 已实现 | `RunValidator`、`ShardPlanner`、`RunPlanWriter` |
| 派工 pump | 部分实现 | 只派首批 `permitsPerRun`，无补派循环 |
| worker 进程内 E/T/L | 骨架 | `ShardExecutor`、`JdbcSourceConnector`、`JdbcSinkWriter` |
| checkpoint 续跑 | **未实现** | 不写 chunk cursor、不读 `resumeCursor` |
| 数据+checkpoint 原子提交 | **未实现** | Sink 与 Checkpoint 各自连接/事务 |
| 目标表 PK/幂等/upsert | **未实现** | 普通 INSERT，无 PK/唯一键 |
| ErrorClassifier/自动重试/DLT 生产 | **未实现** | 无 `ErrorClassifier`；worker 不发 DLT |
| 人工 API（cancel/retry/resume/abort/failures） | **未实现** | `EtlTaskController` 只有 CRUD |
| 多源/文件/IoT/stream | **未实现** | SPI 只有 JDBC POSTGRES/MYSQL |
| 安全（凭据不落 Kafka、DB 加密、JWT 治理） | **未实现** | 派工消息带明文 password；secret 硬编码 |
| Orchestrator 指标/告警 | 部分 | 只有 Counter；无 Gauge/orchestrator 告警组 |
| 39 个集成测试 / 58 个单测 | 已存在 | 但 E2E 仅 3 分片/20 行，未覆盖崩溃/续跑/重复 |

### 1.3 完成度校正

v1 文档中“数据面接线已落地”“端到端可运行”“影子切换已落地”等表述需要校正：

| v1 表述 | 代码事实 | v2 口径 |
|---|---|---|
| 数据面接线已落地 | 只派首批 8 片；无补派 | **部分实现** |
| 端到端可运行 | 3 分片/20 行 E2E | **限定规模可用** |
| 失败处置完备 | 无 retry/DLT | **未实现** |
| 影子运行比对 | flag 只有 getter/setter | **未实现** |
| 可观测性闭环 | Phase 0 指标 + 少量日志 | **部分实现** |

---

## 2. 目标架构（target-state）

### 2.1 设计原则

1. **数据面优先**：先证明“不丢、不重、可续、可对账”，再扩状态机/灰度。
2. **checkpoint 是真源**：`results/progress` 只是触发器；终局由 checkpoint 聚合重算。
3. **单一写路径**：`etl_task_run.status` 只经 `transition()`；checkpoint 由 worker 写、orchestrator 只读聚合。
4. **DB 约束优先**：单当前 run、shard 唯一、attempt 唯一、行级对账 CHECK 尽量下沉到 DB。
5. **at-least-once + 幂等**：不宣称 exactly-once；所有数据面写入必须可重放。
6. **安全默认**：凭据不落消息、不落日志；DB 加密；密钥可轮换。
7. **低基数可观测**：taskId 只进日志/DB，不进指标标签。
8. **先小后大**：先用 1 个 dataSource 灰度，再 25/50/100。

### 2.2 目标组件

| 组件 | 目标职责 | 现状 |
|---|---|---|
| gateway | JWT/RBAC、路由 `/api/tasks/**`、`/api/data-sources/**` | 路由前缀需修正 |
| orchestrator（task-management） | submit/validate/dispatch/converge/cancel/retry/failures；只编排不处理数据 | 状态机已实现；API/补派/许可待补 |
| data-source-service | 数据源元数据 CRUD；凭据加密/短期令牌 | 仍明文 |
| etl-worker（batch profile） | 进程内 Source→Transform→Sink；chunk 提交；checkpoint；progress/results/DLT | 骨架；原子性/续跑/重试待补 |
| etl-connector-spi | Source/Sink 接口；JDBC 参考实现；目标类型与源类型解耦 | 需拆 source/sink 选择 |
| PostgreSQL | `etl_task`/run/checkpoint/dlt/audit/outbox；目标库 | 05/06 已有；07/08/09 待建 |
| Kafka | 控制面 dispatch/progress/results/dlt/commands | 已有 topic 配置；DLT 生产端待补 |
| Redis | 分布式锁/热点缓存/旧链路展示缓存 | 旧链 Redis 无 TTL 待修 |
| monitoring | 指标/Gauge/告警/面板/runbook | 需补 orchestrator 组 |

### 2.3 批处理数据流（target）

```text
POST /api/tasks
  → 建 etl_task + etl_task_run(CREATED)
  → 异步 validate（V-1..V-4）
  → 生成 shard 计划（分片数=并行度，chunk=事务单元）
  → QUEUED
  → DispatchService 按 DB 许可持续补派 PENDING
  → worker 领取 shard：SourceReader → Transformer → SinkWriter
      每个 chunk：目标写入 + checkpoint(last_cursor) 同事务
      每个 shard 结束：results
  → orchestrator 聚合 checkpoint → SUCCEEDED/PARTIAL/FAILED/NEEDS_ATTENTION
  → failures/DLT/audit 可查
```

### 2.4 控制面状态模型

- 保留 12 态/32 边；**Phase 1 不新增状态、不新增边**。
- 新增“收敛检查”作为 sweeper 的一段逻辑，不新增 `CONVERGING` 状态。
- lease/fencing 用 checkpoint 列表达（`lease_owner`、`lease_until`、`shard_attempt`），不新增 shard 状态。
- `cancel_requested_at` 继续表达软取消；不新增 `CANCEL_REQUESTED` 状态。

### 2.5 Phase 1 明确不做

- Mongo/File/GeoJSON/IoT/stream runtime。
- 独立 Flink/Iceberg/湖仓。
- 多租户、跨集群 DAG、复杂权限模型。
- 完整 outbox/audit 的全部运维能力（Phase 1B 只做最小闭环）。
- 生产 HA 集群（先明确单节点风险与备份/恢复）。

---

## 3. 关键决策（Decision Log）

| ID | 结论 | 理由 | 被否方案 |
|---|---|---|---|
| D1 | 保留 12 态/32 边，Phase 1 不再扩状态机 | 已实现且有测试；当前瓶颈在数据面 | 推翻重做/新增状态 |
| D2 | 目标表 Phase 1 就加 PK + `idempotency_key UNIQUE` + `ON CONFLICT` | 没有 PK 的重试必然重复；Phase 2 太晚 | 先跑通再补 PK |
| D3 | 明确提交边界：同库用本地事务；跨库用幂等写入 + checkpoint after commit + 重放安全；每 chunk 写 `last_cursor`；worker 从 `resumeCursor` 续跑 | 跨库不应引入 XA/2PC；“断点续传”必须可读可写 | 只写 checkpoint 不读、跨库 2PC |
| D4 | 分片数=并行单元（8~32/任务），chunk=事务单元（5k~20k） | 避免 6000 分片/任务的控制面爆炸 | 一 chunk 一 shard |
| D5 | 全局许可用 DB `COUNT(ASSIGNED/RUNNING)` 或 Redisson 信号量 | JVM Semaphore 不全局、不抗重启 | 继续用本地 Semaphore |
| D6 | 派工 key=`runId+shardId`；worker 配 `concurrency`/`max-poll-records`/异步执行 | 修复 I1；避免 rebalance 重投 | taskId 作 key、单线程 |
| D7 | 派工/结果发送至少等待 ack；Phase 1B 切最小 outbox | 异步 send 丢消息无法感知 | 只记 ERROR 日志 |
| D8 | 引入 `ErrorClassifier` + 分片内重试 + T12/T21 + DLT 落表 | 失败处置是 Phase 1 核心 | 异常直接 FAILED |
| D9 | `rows_filtered` 与 `rows_failed` 分离；`SUCCEEDED` 不允许 failed>0 | 防止静默丢数被判成功 | 继续用 `rowsDropped` 混用 |
| D10 | 凭据只传 `dataSourceId`；DB 加密；JWT secret 外置 | 安全是出厂门槛，不是 Phase 2 | 派工消息带明文 password |
| D11 | 取消/重试/恢复/失败查询 API 最小闭环进 Phase 1B | NEEDS_ATTENTION 必须可操作 | 全部推到 Phase 2 |
| D12 | 灰度按 dataSource + shadow compare；回滚需排空在途 run | 全局布尔开关不可灰度 | `newWorker` 全开全关 |
| D13 | 2 周 spike 评估 Spring Batch/Temporal；未通过则切换 | 自研可靠性轮子风险高；补充件已推荐 | 先写完再评估 |
| D14 | 源端一致性用高水位线 + 明确语义（快照/尽力而为） | 无快照的跨分片扫描无法保证不漏不重 | 默认强一致 |
| D15 | sink 按 `targetDataSourceId` 选择，不复用 worker 本地 DataSource；MySQL 源→PG 目标必须回归 | 现有 `ConnectorConfig` 按源类型注册 sink，可能写错库 | 沿用源类型选 sink |

---

## 4. 阶段路线图

> 时间按“10 人、3 个并行 stream、无独立 SRE”估算；关键路径（崩溃续跑、幂等、30M 压测、灰度观察）不因加人而压缩。每个阶段必须有可验证的退出条件，不以“代码写完”作为完成。

| 阶段 | 时间盒（10 人） | 关键路径 / 不可压缩项 |
|---|---|---|
| Phase 0 | 1 周 | 基线、端口、路由 |
| Phase 1A | 3 周 | 崩溃续跑 + 幂等 + 30M 压测；10 人并行但关键路径不变 |
| Phase 1B | 2~3 周 | 多实例、收敛、API、回滚；可与 Phase 1A 后半段并行设计 |
| Phase 1C | 4 周 | 1 个 dataSource 7 天 + 25/50/100 各 1 周；观察期不可压缩 |
| Phase 2 | 4~6 周 | 多源 connector/治理；可并行 2 个 connector stream |
| Phase 3 | 4~6 周（条件） | stream runtime；业务未确认则跳过 |
| Phase 4 | 1~2 周 | 旧链路 drain/删除；依赖 100% 运行 ≥2 周 |

> 10 人不是把 Phase 1 砍到 5 周：开发部分约 6 周，灰度观察 4 周，合计约 10 周；多出的人力用于并行测试、安全、可观测性和 Phase 2 预研。

### Phase 0 · 收尾与基线（1 周）

**目标**：把 v1 Phase 0 的欠账补齐，给后续改动一个可比较的基线。

**交付物**

- 旧 Redis `task:status:{taskId}` 加 TTL（建议 24h）。
- 复制一份修复后的基线：P50/P95/P99 任务时长、行/秒、Full GC、源库 CPU/连接、Kafka lag。
- worker 端口改为独立端口（避免与 user-service 8086 冲突），加入 `config-repo/etl-worker.yml` 与 Prometheus target。
- 修正 gateway 路由前缀（`/api/tasks/**`、`/api/data-sources/**`）。
- 关闭 v1 文档中与实现不符的“已完成”标记，链接评审报告。

**退出条件**

- 基线数据落 `docs/architecture/performance-baseline-2026-10.md`（或等价文档）。
- 本地启动栈无端口冲突；worker 指标可被抓取。

**执行状态（2026-10-19）**

- [x] 旧链路 Redis `task:status:*` 增加 24h TTL（可配 `etl.legacy.redis-status-ttl-hours`）。
- [x] worker 端口改为 8087；新增 `config-repo/etl-worker.yml`；Prometheus 增加 8087 target。
- [x] gateway 路由改为 `/api/tasks/**`、`/api/data-sources/**`，并修正 health 路由顺序。
- [x] 建立 `docs/architecture/performance-baseline-2026-10.md` 采集模板与 SQL/PowerShell 命令。
- [x] `mvn test -DskipITs` 全 reactor 通过（12 模块，BUILD SUCCESS）。
- [x] 基线数值已采集（legacy 链路，`newWorker=false`）：4 次 warm 样本 P50 45.57s、P95 49.73s、吞吐 427.9 行/s，冷启动 95.84s；Redis TTL≈24h；任务结束后 Kafka lag=0。详见基线文档 §7。
- [x] gateway 运行时回归：`/api/tasks`、`/api/data-sources`、`/api/data-sources/actuator/health` 均 200，无 token 请求 401。

### Phase 1A · 数据面正确性 Gate（3 周，最高优先级；10 人并行不压缩关键路径）

**目标**：证明“30M 行、>100 分片、崩溃/重试/重复投递下不丢不重、可续跑、可对账”。

**工作包**

1. **WP-01 全量派工**：`DispatchService` 对 `DISPATCHED/RUNNING` 且仍有 PENDING 的 run 持续补派；`QUEUED→DISPATCHED` 仅一次；令牌不足时部分获取而非全有或全无。
2. **WP-02 提交边界 + 续跑**：同库时 sink 与 checkpoint 共享本地事务；跨库时目标幂等提交后再写 checkpoint，崩溃重放安全；每 chunk 写 `last_pk/last_cursor`；`JdbcSourceConnector.open` 从 `resumeCursor` 之后读；边界去重。
3. **WP-03 幂等与目标连接**：目标表建 PK + `idempotency_key UNIQUE`；`INSERT ... ON CONFLICT DO UPDATE/NOTHING`；明确 `targetDataSourceId`，sink 不复用 worker 本地 DataSource；Source/Sink 解耦并回归 MySQL→PG；`rows_deduped` 语义明确。
4. **WP-04 丢数可见**：`JdbcSinkWriter.writeRowByRow` 显式 commit；拆 `rows_filtered`/`rows_failed`；坏行写 `etl_task_dlt`（新增 `07-etl-task-dlt.sql`）；failed>0 不得 `SUCCEEDED`。
5. **WP-05 失败处置**：`ErrorClassifier`（TRANSIENT/PERSISTENT/FATAL）+ 分片内重试 + T12/T21 + DLT 生产端；worker 不再把所有异常标 PERSISTENT。
6. **WP-06 安全**：派工只带 source/target `dataSourceId`；worker 通过 data-source-service 获取短期凭据；DB 口令加密；JWT secret 外置；Kafka payload 扫描无明文口令。
7. **WP-07 数值/空间**：取消默认 2 位小数；按列精度；`PGobject`/`ST_GeomFromText(?,4326)`；支持 MultiPolygon/GeometryCollection；MySQL→PG 类型映射。
8. **WP-08 并行消费**：派工 key 改 `runId+shardId`；worker 配置 concurrency/max-poll；长分片异步执行 + pause/resume。
9. **WP-09 测试**：>100 分片、崩溃续跑、重复投递、坏行 DLT、PostGIS、MySQL 类型、MySQL→PG sink、1 任务 8 分片 2 worker。

**退出条件（Gate 1）**

- 评审报告 §9.1 的 1～9 项全部通过。
- 30M 行、>100 分片、20 并发任务压测通过（或给出可解释的容量上限）。
- 崩溃后目标行数 = 源行数，无重复；重复投递不产生重复数据。
- 安全扫描：Kafka payload/日志/DB 无明文凭据。
- **未通过则不得开启 `newWorker=true`。**

**执行状态（2026-10-19，本地可执行部分）**

- [x] **WP-01 全量派工**：pump 扫描 `QUEUED/DISPATCHED/RUNNING` 且仍有 PENDING 的 run 持续补派；令牌不足时部分获取。证据：`DispatchServicePumpTest`（9 例）+ `FullDispatchScaleIT`（120 分片，一次 pump 后 `COUNT(PENDING)=0`、全部 `ASSIGNED`）。
- [x] **WP-02 提交边界+续跑**：chunk 级 `last_cursor/last_pk`、`resumeCursor` 开区间读、同库同事务。证据：`ShardExecutorTest` 的续跑/同库事务用例；`FullChainE2EIT.crashAfterTargetCommitReplaysIdempotently` 在真 PG 上预置“目标已提交、checkpoint 未写”，重放后目标仍 20 行、`rows_deduped=7`、无重复。
- [x] **WP-03 幂等与目标连接**：目标表 `idempotency_key` 主键 + PG `ON CONFLICT DO NOTHING` / MySQL `ON DUPLICATE KEY UPDATE`；明确 `targetDataSourceId`。证据：`JdbcSinkWriterSqlTest`、`IdempotencyKeysTest`、`ShardExecutorTest`。
- [x] **WP-04 丢数可见**：`rows_filtered/rows_failed` 拆列、`07-etl-task-dlt.sql`、逐行显式 commit、`rows_failed>0` 不得 `SUCCEEDED`。证据：`ShardExecutorTest`、`KafkaDataPlaneIT` 的 DLT→NEEDS_ATTENTION 用例、`GuardChainTest` G-7。
- [x] **WP-05 失败处置**：`ErrorClassifier`（TRANSIENT/PERSISTENT/FATAL）+ 分片内退避重试 + T12/T21 + worker DLT 生产。证据：`ErrorClassifierTest`（7 例）、`ShardExecutorTest`（瞬态重试/失败 DLT）、`DltListenerTest`。
- [x] **WP-06 安全**：派工消息只带 `dataSourceId/targetDataSourceId`（`EntryChainIT` 断言 payload 无 `sourceUrl/password/jdbc:`）；DB 口令 AES/GCM 加密与公开接口脱敏（`PasswordCipherTest`/`DataSourceServiceTest`）；内部凭据端点不经网关；JWT secret 改为环境变量注入。
- [x] **WP-07 数值/空间**：取消默认 `setScale(2)`、按列精度、PG `ST_GeomFromEWKT(?,4326)`、MultiPolygon/GeometryCollection、MySQL→PG 类型映射。证据：`NumberScaleTransformerTest`、`GeoJsonToWktTransformerTest`、`TypeMappingTest`。
- [x] **WP-08 并行消费**：dispatch key=`runId:shardId`；worker 消费组可配置（`etl.worker.consumer-group`）；IT dispatch topic 多分区。证据：`KafkaDispatchPublisherTest`、`TwoWorkerConcurrencyIT`（1 任务 8 分片、2 worker，任务前后 group 分配快照一致、无重复）。
- [x] **WP-09 测试**：`mvn test` 全反应堆 134 例通过；`-Pit` 43 例通过、**0 skipped**（真 PG/Kafka/Redis，含 `FullChainE2EIT` 真搬 20 行与 `TwoWorkerConcurrencyIT`）。
- [x] **G-4 口径修正（评审 P1-4）**：对账改为 `rows_out + rows_filtered + rows_failed + rows_deduped = rows_in`，同步 `RunAggregate/GuardChain/§12.2 校验 2/设计文档`；`ConvergencePolicyTest` 与 `ConvergenceGuardIT` 覆盖“崩溃重放 deduped 入账”。
- [ ] **30M 行 / 20 并发任务 / >100 分片压测**：尚未执行（环境型证据）。
- [ ] **MySQL→PG / PostGIS 真库回归**：尚未执行（本地 compose 无 MySQL；PostGIS 扩展未纳入 IT 源/目标链路）。
- [ ] **进程级故障注入**：当前用“目标已提交、checkpoint 未写”等价注入；真实 `kill -9` worker / broker 重启待补。
- [ ] **安全扫描报告**：Kafka payload 与 DB 加密已有测试；全量日志扫描与 RBAC（cancel/retry/forgive）归 Phase 1B。

> 结论：Phase 1A 的代码与本地可执行验收已收口；Gate 1 的环境型证据（30M 压测、MySQL/PostGIS 回归、进程级故障注入）仍未完成，`etl.routing.new-worker` 继续默认 `false`，不得开启灰度。

### Phase 1B · 控制面收口与可运维（2~3 周）

> 可在 Phase 1A 后半段并行设计 API/告警/灰度契约，但 Gate 2 必须在 Gate 1 之后。

**目标**：让新链路可操作、可观测、可回滚；不新增状态。

**工作包**

1. **WP-10 收敛与守卫**：`ConvergenceSweeper`（无活跃分片时重算）；T31 幂等；G-2 总和校验；T19 failed=0/无未解决 DLT；G-7 DLT_CLEAN。
2. **WP-11 lease/fencing**：checkpoint 加 `lease_owner/lease_until`；写入带 `shard_attempt/lease` 条件；`/resume` 重置过期 lease 并写 audit。
3. **WP-12 全局许可**：用 DB `COUNT(ASSIGNED/RUNNING)` 或 Redisson 信号量；去掉 JVM `Semaphore` 的全局假设。
4. **WP-13 目标表治理**：V-3 真正验证/创建目标表（advisory lock）；worker 不再每分片建表；类型映射/列名引用；存量目标表的 PK/唯一索引迁移与重复数据清洗。
5. **WP-14 API 最小闭环**：`GET /progress`、`POST /cancel`、`GET /failures`；`/retry`、`/resume`、`/abort` 至少留出契约与 409 语义。
6. **WP-15 观测**：`etl.run.state.count`、`etl.checkpoint.lag.seconds`、`etl.dlt.unresolved.size`、`etl.sweeper.actions.total`；`etl-orchestrator-alerts` 告警组；MDC runId/shardId/attemptNo。
7. **WP-16 迁移能力**：dataSource 白名单灰度；shadow compare（行数/校验和/终态/耗时）；回滚 runbook（停提交→排空/取消→切旧链路）。
8. **WP-17 源端一致性**：高水位线/快照语义；MySQL 流式 fetch；查询超时/取消。
9. **WP-18 契约与文档**：dispatch/results JSON 契约测试；修正 `orchestrator-design.md` 的 dispatch key/lease/DLT/DB 许可；更新 `connector-spi.md`。

**退出条件（Gate 2）**

- 2 个 orchestrator + 2 个 worker 多实例测试通过；令牌桶/lease 全局有效。
- 结果丢失、ASSIGNED-only、T31 重放、旧 worker 迟到写入均有测试。
- 告警可触发；`/progress`/`/cancel`/`/failures` 可用；网关路由可达。
- 回滚演练通过：无孤儿 run、无重复写入。

**执行状态（2026-10-19，本地可执行部分）**

- [x] **WP-10 收敛与守卫**：`ConvergenceSweeper` 无 results 时重算聚合；T31 重放删旧计划再插入（`RunPlanWriter`）；G-2 总和、G-7 DLT_CLEAN/T19 failed=0 在 `GuardChain`/`ConvergencePolicy`。证据：`SweeperIT`（含“结果丢失收敛”“计划已落库 T31 重放”）、`ConvergenceGuardIT`。
- [x] **WP-11 lease/fencing**：`CheckpointWriter.tryAcquireLease/renewLease` 带 `lease_owner + lease_until + shard_attempt` 三重条件；lease 接管后旧 worker 迟到写入被拒；`/resume`/`/retry` 用 `__reclaimed__` 哨兵回收 lease。证据：`LeaseFencingIT`、`CheckpointWriterSqlTest`、`ShardExecutorTest`。audit 表落库仍归 Phase 2（当前为结构化 WARN 日志）。
- [x] **WP-12 全局许可**：`TokenBucket` 改为 DB `COUNT(ASSIGNED/RUNNING)` 真源，`DispatchService.dispatchOne` 事务内取 PG advisory lock；容量按配置动态读取。证据：`GlobalPermitIT`（两个 orchestrator 等价实例共享同一份许可）、`EntryChainIT` T6、`FullDispatchScaleIT` 120 分片。
- [x] **WP-13 目标表治理**：`TargetTableManager` V-3 在目标库 advisory lock 下一次性建表/补 `idempotency_key` 唯一索引；worker 删除每分片 `ensureTargetTable`。存量重复键不在线自动删数据，建唯一索引失败即 V-3 失败并提示清洗。证据：`TargetTableGovernanceIT`、`FullChainE2EIT`（目标表由 V-3 建出后真搬 20 行）。
- [x] **WP-14 API 最小闭环**：`GET /progress`、`GET /failures`、`POST /cancel|retry|resume|abort|mark-succeeded-with-data-loss`；无 current run→404、非法状态/guard→409。证据：`EtlTaskControllerTest`；网关 `gateway-service-routes.yml` 已有 `/api/tasks/**`→`lb://task-management-service`。
- [x] **WP-15 观测**：`RunStateMetrics` 产出 `etl.run.state.count`/`etl.checkpoint.lag.seconds`/`etl.dlt.unresolved.size`；各 sweeper 产出 `etl.sweeper.actions.total`；`alerts.yml` 新增 `etl-orchestrator-alerts`（NEEDS_ATTENTION/DLT/checkpoint lag）；Kafka 监听器 MDC 补 `runId/shardId/attemptNo`，logback pattern 同步。告警实际触发仍需 Prometheus/Alertmanager 环境。
- [x] **WP-16 迁移能力**：白名单 `etl.routing.data-source-whitelist` + `isNewWorkerFor` 已在 Phase 1A 落地；新增 `ShadowCompareService`（旧 `completed_records` vs 新 `rows_out`，仅 shadow-compare=true 且旧计数>0）；回滚 runbook 见 `docs/architecture/e2e-runbook.md`。真实回滚演练未执行（需环境）。
- [x] **WP-17 源端一致性**：分片计划冻结 `pkTo`（主键高水位），run 期间新行不进入本次 run；`SourceSpec.queryTimeoutSeconds` + MySQL `Integer.MIN_VALUE` 流式 fetch；`WorkerProperties.query-timeout-seconds` 默认 600s。证据：`SourceSnapshotIT`。源端取消仍靠 lease/超时，未做服务端 cancel 协议。
- [x] **WP-18 契约与文档**：`KafkaContractTest` 锁 dispatch/results/progress/dlt 字段与“派工不含连接/口令”；`orchestrator-design.md`、`connector-spi.md`、`AGENTS.md` 同步 lease/DB 许可/V-3 约束。
- [x] **Gate 2 环境型证据（2026-10-19）**：两个独立 task-management JVM（8082/8092，共享 DB）在 `max-concurrent-shards=4` 下稳定保持 4 个 ASSIGNED，Kafka 每 shard 恰好 1 条派工消息；杀掉一个实例后另一个继续补派。两个独立 worker 进程（不同消费组）验证了 lease 互斥、接管与旧 owner 迟到写入被拒。Prometheus/Alertmanager 实际触发 `EtlCheckpointLagHigh`、`EtlRunNeedsAttention`、`EtlDltUnresolved`，webhook 收到投递。回滚演练（`new-worker=false` + `/abort` T26）后无孤儿 run、无重复写入，并用旧链路完成任务 `2883cdda...`（100 行）验证接管。环境验证发现并修复 `globalMaxUpdatedAt` 未限定活跃 run 导致空闲误报 lag 的缺陷，新增 `CheckpointLagMetricIT`。证据：`e2e-logs/phase1b-gate2-evidence.md`。

> 结论：Phase 1B 的代码与 Gate 1/2 本地证据已收口（`mvn test` 141 例、`-Pit` 51 例，0 failures/0 skipped）；`etl.routing.new-worker` 默认仍为 `false`，Phase 1C 按 `data-source-whitelist` 逐个 dataSource 开启。

### Phase 1C · 灰度（4 周：1 个 dataSource 观察 7 天 + 3 个放量阶段各 1 周）

**目标**：在真实业务上验证新旧链路一致性和回滚能力。

**步骤**

1. 选 1 个 `dataSourceId`，`newWorker=true` + 白名单；其余 dataSource 仍走旧链路。
2. 影子比对：目标表行数、分片 checksum、终态、耗时；每日报表。
3. 通过后 25% → 50% → 100%；每阶段观察 1 周。
4. 任一 P0 指标异常：停止新提交 → 排空/取消在途 run → `newWorker=false` → 旧链路接管。

**退出条件（Gate 3）**

- 连续 7 天无数据差异、无 P0 告警、无孤儿 run。
- 回滚演练在真实 dataSource 上通过。
- 旧链路至少保留一个可回退版本。

### Phase 2 · 多源与治理（4~6 周，Gate 3 之后）

**范围**

- Mongo/File(JSON/CSV/GeoJSON) connector；在 Phase 1A 已解耦的 Source/Sink 上扩展。
- 目标表治理：命名/保留/清理/血缘；一任务一表的策略复核。
- Schema Registry/契约（可选，按团队能力）。
- audit 表与人工操作审计；outbox 完整化（若 Phase 1B 只做了最小闭环）。
- 数据质量：行数/checksum/抽样比对；nightly 校验落定时任务。
- MySQL/PostGIS 真实样本回归。

**明确不做**：独立 Flink/湖仓/多集群。

### Phase 3 · 流式通道（条件触发，4~6 周）

**触发条件**：IoT/CDC/文件监听有明确业务需求，且 Phase 2 的批处理稳定运行 ≥1 个月。

**范围**

- `etl-worker` stream profile；`source.iot.raw.*`/`source.file.arrival`。
- 常驻 run 的状态语义（不适用 G-2/G-4）；静默源与卡死的区分。
- Kafka lag/offset checkpoint；pause/resume。

**若业务未确认，Phase 3 直接跳过，不为了“架构完整”而做。**

### Phase 4 · 旧链路下线（1~2 周）

**前置条件**

- 新链路 100% dataSource 运行 ≥2 周。
- 旧 topic 连续 7 天无生产、lag=0。
- 旧服务无在途任务；旧目标表已归档/确认。

**动作**

- 停止旧 `TaskScheduler`/旧监听器；删除旧 topic；从父 pom 移除 E/T/L 三服务。
- 保留 3 个月发布产物与配置；打 `git tag pre-etl-merge-v1`。
- 更新 `architecture-overview.md`、`README.md`、`AGENTS.md`、运行手册。

---

## 5. 工作分解与角色（10 人假设）

| 角色 | 人数 | 主要职责 | 对应工作包 |
|---|---|---|---|
| A · 编排/控制面 | 2 | 状态机、sweeper、dispatch、API、灰度/回滚 | WP-01、WP-10、WP-11、WP-14、WP-16 |
| B · 数据面/连接器 | 3 | worker、checkpoint、sink/source、幂等、DLT | WP-02、WP-03、WP-04、WP-05、WP-08、WP-13 |
| C · 平台/安全/观测 | 2 | PG/Kafka/Redis、密钥、指标、告警、压测 | WP-06、WP-12、WP-15、WP-17 |
| D · 测试/质量 | 2 | IT/故障注入/契约测试/验收脚本 | WP-09、Gate 1/2/3 |
| E · 产品/运维 | 1 | 业务口径、失败处置流程、回滚决策、on-call | WP-16、Gate 3、Phase 4 |
| **合计** | **10** | | |

**并行 stream（10 人）**

| Stream | 人员 | 负责工作包 | 依赖 |
|---|---|---|---|
| S1 数据面正确性（关键路径） | B×2 + C×1 + D×1 | WP-01～05、08、09 | 无 |
| S2 安全/目标连接/性能 | B×1 + C×1 + D×1 | WP-06、07、17 + 30M 压测 | S1 的 sink/checkpoint 接口 |
| S3 控制面/API/灰度 | A×2 + E×1 | WP-10～16、18 | S1 的 checkpoint 语义；Gate 1 后全面展开 |

> Gate 1 后，S1/S2 的主力并入 S3，优先 WP-11～15、18；S3 保留 1 人继续灰度准备。
>
> 10 人协作：每 2 周一个集成点；S1 对 Gate 1 负责，S3 对 Gate 2 负责；数据/状态路径改动需 2 人 review；30M 压测只做一次统一环境，避免各 stream 重复占资源。
>
> 如果实际可用人力 <6：回退到 5 人基线（Phase 1A/1B 各 +1 周）；<4：只做 WP-01～WP-06，Phase 2/3 延后。

---

## 6. 验收门槛与度量

### Gate 1 · 数据面正确性（Phase 1A 结束）

- 评审报告 §9.1 的 1～9 项全部通过。
- 30M 行/20 并发/4C8G 压测：无 OOM、无 Full GC 风暴、无任务卡死；P95 相对基线有明确改善或给出容量上限。
- 压测在源表静态或快照隔离下进行；源表并发写语义在 Gate 2 前关闭（评审 §9.1-13）。
- 崩溃/重试/重复投递：目标行数 = 源行数；无重复；G-4 与 checksum 一致。
- 失败处置：瞬态重试成功、持久退避、FATAL 进 DLT；`rows_failed>0` 不判 SUCCEEDED。
- 安全：Kafka/日志/DB 无明文凭据。

### Gate 2 · 可运维/多实例（Phase 1B 结束）

- 评审报告 §9.1 的 10～14 项全部通过。
- 2 orchestrator + 2 worker；无令牌桶饥饿/超发；无 rebalance 双跑。
- 结果丢失、ASSIGNED-only、T31 重放、旧 worker 迟到写入测试通过。
- `/progress`/`/cancel`/`/failures` 可用；网关路由可达；告警触发。
- 回滚演练通过。

### Gate 3 · 灰度（Phase 1C 结束）

- 1 个 dataSource 连续 7 天：行数/校验和/终态/耗时无差异。
- 25/50/100 每阶段观察 1 周；无 P0 告警。
- 回滚真实演练通过；旧链路可回退。

### Gate 4 · 旧链路下线（Phase 4）

- 新链路 100% ≥2 周；旧 topic 7 天无生产、lag=0；无在途任务。
- 旧服务删除后，监控/告警/runbook 全部指向新链路。

### 关键指标

- `etl.run.state.count{status}`、`etl.checkpoint.lag.seconds`、`etl.dlt.unresolved.size`。
- 任务 P50/P95/P99、行/秒、失败率、重试率、DLT 率、`rows_failed` 率。
- 源库 CPU/连接数/慢查询；Kafka lag；worker GC。
- 新旧影子比对：行数差、checksum 差、终态差、耗时差。

---

## 7. 风险与不推进条件

| 风险 | 影响 | 缓解 |
|---|---|---|
| 自研可靠性轮子成本超预期 | Phase 1A 延期、线上故障 | 2 周 spike；不通过则 Spring Batch/Temporal |
| 30M 行/20 并发超容量 | 任务卡死/OOM/源库过载 | 压测、分片/ chunk 调参、源库限流 |
| 源表并发写 | 漏行/重复/对账不平 | 高水位线/快照语义；明确假设 |
| PostGIS/GeoJSON 类型复杂 | 主场景失败 | 真实样本、GeoTools/PGobject、SRID 契约 |
| 凭据治理阻塞 | 无法上线 | Phase 1A 就做；Vault/KMS/Jasypt 备选 |
| 灰度回滚不干净 | 数据双写/孤儿 run | dataSource 白名单、排空/取消 runbook、演练 |
| 团队容量不足 | 质量下降 | 缩小 Phase 1 范围；Phase 2/3 延后 |

**不推进（No-Go）条件**

- Phase 1A 结束时，崩溃续跑/重复投递/全量派工仍不能稳定通过。
- 30M 行/20 并发压测出现不可接受的 OOM/卡死/源库过载。
- 安全项无法在 Phase 1A 关闭。
- 灰度出现无法解释的数据差异或重复写入。
- 团队无法承担 12 态 + 4 sweeper + outbox 的长期维护（则切换框架）。

---

## 8. 文档与代码联动

| 文档/代码 | 动作 | 时机 |
|---|---|---|
| `docs/architecture/etl-architecture-review-2026-10-19.md` | 评审基线，保持只读 | 已保存 |
| `docs/architecture/etl-architecture-evolution-plan.md` | 本文件（v2） | 已更新 |
| `docs/architecture/orchestrator-design.md` | 更新 dispatch key、lease 字段、DLT 表、DB 许可、Phase 1 范围；不新增状态 | Phase 1A/1B 实现时 |
| `docs/architecture/target-lifecycle-states.dot` | 仅在状态/边变化时更新；v2 不变 | 如变更 |
| `docs/architecture/connector-spi.md` | Source/Sink 解耦、resumeCursor、目标类型映射 | Phase 1A |
| `db/schema/07-etl-task-dlt.sql` | DLT 表 | Phase 1A |
| `db/schema/08-etl-task-audit.sql` | audit 表 | Phase 1B |
| `db/schema/09-etl-outbox.sql` | 最小 outbox | Phase 1B |
| `config-repo/task-management-service.yml` | dataSource 白名单、dispatch key、许可、DLT、指标 | Phase 1A/1B |
| `config-repo/etl-worker.yml` | worker 端口、concurrency、max-poll、transform 配置 | Phase 0/1A |
| `monitoring/prometheus/rules/alerts.yml` | orchestrator 告警组 | Phase 1B |
| `monitoring/grafana/dashboards/etl_task_overview.json` | 任务/分片/DLT/延迟面板 | Phase 1B |
| `AGENTS.md` | 新构建/测试/运行注意事项 | Phase 1A 完成后 |

---

## 9. 开放问题（需产品/运维确认）

1. 源表在 ETL 期间是否允许并发写？采用快照还是高水位线？
2. 峰值并发任务/分片数、每源库连接上限是多少？
3. orchestrator/worker 是否多实例部署？单实例是否可接受？
4. orchestrator 与 worker 是否始终共享同一 PostgreSQL？
5. 目标表保留/清理/血缘策略是什么？
6. 凭据治理用 Vault/KMS/Jasypt 哪一种？是否允许 worker 动态取凭据？
7. `PARTIAL_SUCCESS`/数据丢失的业务容忍度与宽恕权限归谁？
8. NEEDS_ATTENTION 的值班/升级/SLA 是什么？
9. 是否允许引入 Spring Batch/Temporal？团队能否维护？
10. 性能基线/目标 P95/吞吐/容量是多少？
11. 回滚窗口与在途 run 的处理策略是什么？
12. 源类型/PK 类型矩阵（无 PK、字符串 PK、复合 PK、分区表、视图）是什么？
13. 目标 schema/type mapping 契约（SRID、precision、timezone、MySQL→PG）由谁定义？
14. PG/Kafka/Redis 的 HA/备份/恢复要求与 RPO/RTO 是什么？
15. 是否接受手工 JSON 契约，还是引入 Schema Registry？

---

## 10. 最初 10 个工作日

| 天 | S1 数据面正确性（关键路径） | S2 安全/目标连接/性能 | S3 控制面/API/灰度 |
|---|---|---|---|
| 第 1 天 | 保持 `newWorker=false`；确认样本/角色 | 密钥/目标库/压测环境确认 | API/灰度/回滚范围确认 |
| 第 2 天 | 采基线；列出 P0 对应代码 | 安全基线/凭据扫描 | 现有 API/网关/告警盘点 |
| 第 3–5 天 | WP-01 全量派工 + WP-02 提交边界/续跑 | WP-06 凭据治理 + WP-07 数值/空间 | WP-10/11/14 设计；路由/白名单设计 |
| 第 6–7 天 | WP-03 目标幂等 + WP-04 坏行 DLT | WP-17 源端一致性 + MySQL→PG 回归 | WP-12/13/15 设计；shadow compare 设计 |
| 第 8 天 | WP-05 ErrorClassifier/重试 | 30M 压测数据/环境准备 | WP-16 回滚 runbook 初稿 |
| 第 9 天 | WP-08 并行消费 + WP-09 测试 | 30M 压测执行 | Gate 1 证据清单/评审材料 |
| 第 10 天 | Gate 1 预评审（S1 主导） | 压测/安全报告 | 决策：继续自研/切 Spring Batch/Temporal |

> 第 10 天是 Gate 1 预评审，不是最终 Gate 1；最终 Gate 1 在 Phase 1A 第 3 周末。没有崩溃续跑/重复投递/全量派工证据，不进入 Phase 1B。

---

## 11. 变更记录

- **2026-10-19 v2.0**：按 `etl-architecture-review-2026-10-19.md` 重制；Phase 1 改为“数据面正确性 Gate”；团队假设由 5 人更新为 10 人；补 `targetDataSourceId`、跨库提交边界、DLT/幂等提前；Phase 1C 明确为 4 周灰度。
- **v1**：旧版演进计划，见 git 历史。
