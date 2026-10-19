# ETL 架构演进方案评审报告（2026-10-19）

> 评审对象：`docs/architecture/etl-architecture-evolution-plan.md`（评审时为 v1；v2 已按本报告重制）。
> 交叉核对：`orchestrator-design.md`、`architecture-overview.md`、`risk-addendum-and-tech-selection.md`，以及当前 `task-management-service`、`etl-worker`、`etl-connector-spi`、`db/schema`、`config-repo`、`monitoring` 的实际代码与配置。
> 评审方式：文档声明 → 代码/DDL/配置证据 → 一致性判断。未运行 3000 万行压测；性能类结论为结构性推断，代码类结论为直接证据。
> 结论：**不建议按当前版本原样推进到生产灰度或 Phase 2；建议“有条件继续”（Conditional Go）。**
> 团队规模假设更新（2026-10-19）：由 ≤5 人调整为 10 人。P0/P1 结论不变；控制面复杂度的相对成本下降，Phase 1 可并行推进，但 30M 压测与灰度观察期不可压缩。

---

## 1. 总体结论

方案的方向是对的：

- E/T/L 合并为 in-process worker，减少跨服务跳数与中间 Kafka；
- checkpoint 作为完成判定真源，替代 Redis 计数；
- `OrchestratorService.transition()` 作为唯一状态写入口；
- 用 DB 唯一索引/CHECK 表达不变式；
- 文档、决策记录、32 条迁移边和单测覆盖明显高于一般项目。

但当前实现存在**数据面正确性缺口**，不是“偶发失败”，而是“在大表上必然卡住、重复或丢数”：

1. **派工泵只派首批 `permitsPerRun=8` 个分片**，剩余 PENDING 分片在 run 进入 `DISPATCHED/RUNNING` 后不再被派发。
2. **checkpoint 不可续跑**：`last_cursor` 不按 chunk 写入，worker 也完全不读 `resumeCursor`；“断点续传唯一真源”目前是只写不读。
3. **数据写入与 checkpoint 不在同一事务**，且目标表没有 PK/唯一键；崩溃重试会重复写数据，而 `GREATEST` 计数会让 G-4 对账看起来仍然平衡。
4. **失败处置没有实现**：没有 `ErrorClassifier`，没有自动 T12 重试，worker 不生产 DLT；任何异常直接落 `FAILED/PARTIAL_SUCCESS`。
5. **丢数可以判成功**：逐行降级写入未显式 `commit`，且 `rowsDropped` 混用“业务过滤”和“SQL 失败”，`ShardExecutor` 在 dropped > 0 时仍可标 `SUCCEEDED`。
6. **安全项未闭环**：派工消息携带明文源库口令，`data_source.password` 明文存储，JWT secret 硬编码；与补充件“J1/J2/J3 无条件先做”直接冲突。

一句话概括：**这是一份“控制面设计成熟度高于数据面实现成熟度”的方案。** 现在开启新链路，最大的风险不是状态机边错，而是数据面在真实规模下不可恢复、不可对账、不可证明正确。

### 决策建议

| 决策 | 建议 |
|---|---|
| 是否继续架构方向 | 继续；E/T/L 合并 + checkpoint 真源 + 单一 transition 入口值得保留 |
| 是否现在开启生产灰度 | **否**；`etl.routing.new-worker` 保持 `false` |
| 是否进入 Phase 2 | **否**；先关闭 P0 清单 |
| 是否继续自研可靠性轮子 | 先做 2 周数据面 spike；若不能证明“原子提交 + 续跑 + 全量派工 + 幂等”，改用 Spring Batch/Temporal 承担可靠性部分 |
| 何时可灰度 | P0 出厂门槛全部通过后，先 1 个 `dataSourceId` 灰度，再 25%/50%/100% |
| 何时可宣称 30M/20 并发可用 | 通过 3000 万行、>100 分片、20 并发任务的压测与故障注入后 |

---

## 2. 评审范围、证据与边界

- **代码基线**：当前工作区（2026-10-19）。`etl.routing.new-worker` 默认 `false`，生产仍走旧链路。
- **已核对的关键实现**：
  - `task-management-service/orchestrator/`：`EtlTaskStatus`、`AllowedTransitions`、`GuardChain`、`OrchestratorService`、4 个 sweeper、`DispatchService`、3 个新监听器、`RunValidator`。
  - `etl-worker/`：`ShardExecutor`、`CheckpointWriter`、`DispatchBatchListener`、`KafkaWorkerEventReporter`。
  - `etl-connector-spi/`：`JdbcSourceConnector`、`JdbcSinkWriter`、`ShardRanges`。
  - `db/schema/05-etl-task-run.sql`、`06-etl-task-checkpoint.sql`。
  - `config-server/src/main/resources/config-repo/task-management-service.yml`。
  - `monitoring/prometheus` 与 `monitoring/grafana/dashboards`。
- **未验证项**：30M 行真实压测、生产 QPS/P95 基线、多实例/网络分区故障注入、源表并发写场景。
- **置信度**：P0/P1 中的代码/配置结论为高置信；性能与容量结论为中置信，已在条目中标注。

---

## 3. 现状痛点 → 目标方案：对应性与证据

| 痛点 | 方案承诺 | 当前证据（2026-10 基线） | 评审判断 |
|---|---|---|---|
| A1 链路长 | E/T/L 合并 worker，数据面不过 Kafka | `etl-worker` 已建；`etl.routing.new-worker` 默认 `false`（`task-management-service.yml:80-82`），生产仍走旧链路 | 方向合理，未交付 |
| A2 Full GC 风暴 | Phase 0 堆 1G + 真批 + Caffeine | `performance-analysis.md:24` 有修复前 52 次 Full GC；修复后无 GC/QPS 复测 | 已缓解，未验证；不能继续用 A2 作为“必须合并”的主证据 |
| B1 中间失败难定位 | Sleuth + MDC + OTLP | 4 个服务已接 Sleuth；`DispatchBatchListener.java:40` 只放 `taskId`，`runId/shardId` 未入 MDC；无 Tempo/Jaeger | 部分 |
| B2 无 DLT | `DefaultErrorHandler` + DLT | `task-management-service/.../KafkaConfig.java:38-43` 未配置 error handler；worker 无 DLT 生产者 | 未达成 |
| C1 完成判定依赖 Redis | checkpoint 为唯一真源 | `etl_task_checkpoint` 与聚合已实现；但 results 丢失后没有收敛 sweeper；旧 Redis 键仍无 TTL（`DataExtractionService.java:231-240`） | 部分 |
| C2 `COUNT(*)` 快照 | 分片实读行数对账 | `rows_in` 来自 checkpoint；但 `planShards` 仍执行 `MIN/MAX/COUNT(*)`（`JdbcSourceConnector.java:78`）；无源端快照隔离 | 只解决完成判定，不解决源数据一致性 |
| D1 无 checkpoint | 断点续传 | `ShardExecutor.commitChunk` 不写 cursor；`JdbcSourceConnector.open` 不读 `resumeCursor` | 未达成 |
| D2 无幂等 | PK/unique/upsert | `JdbcSinkWriter` 普通 INSERT，无 PK/唯一键（`JdbcSinkWriter.java:110-121`） | 未达成（Phase 2） |
| E1/E2 多源/流式 | Mongo/File/IoT/stream | 仅 JDBC SPI；`ConnectorType` 仅 POSTGRES/MYSQL | 未达成（Phase 2/3） |
| F1/F2 微服务过度 | 合并 | 设计/目标图已画；旧三服务仍在 | 未交付 |
| H1/H2/I1 | 补充件要求先修 | H1 在 `NumberScaleTransformer.java:39-45` 重现；H2 仍要求整型 PK；I1 由 `taskId` 派工键重现 | 风险未闭环 |
| J1-J3 安全 | 凭据不落 Kafka、加密、密钥治理 | `ShardDispatchCommand.java:26-29`、`ShardDispatchAssembler.java:72-77` 带 `sourcePassword`；`user-service.yml:29-30` 硬编码 secret | 未闭环，P0 |

**判断**：

- 在“问题 → 设计意图”层面，方案基本做到了一一对应。
- 在“问题 → 可验收能力”层面，只有约一半成立。
- A2 的证据是修复前数据；C1 旧链路未修；D1/D2/H1/I1/J 是方案自己列出的高风险项，但在 Phase 1 实现中没有闭环，有些甚至在新 worker 里被重新引入。

---

## 4. 规模假设下的过度设计、能力遗漏与迁移可操作性

### 4.1 过度设计：控制面复杂度偏高

在“单团队 ≤5 人（2026-10-19 更新为 10 人，见文首）、单表 ≤30M、并发 ≤20、4C8G”的假设下，方案引入了：

- 12 个 run 状态、32 条迁移边；
- 4 个 sweeper；
- outbox、DLT、audit 三张表；
- 5 个新 Kafka topic；
- 全局令牌桶；
- batch/stream 两个 profile；
- `/retry`、`/cancel`、`/resume`、`/abort`、`/mark-succeeded-with-data-loss` 五个 API。

这些能力在“多团队、跨集群、高并发”场景下合理；在 20 个并发任务、10 人团队、无独立 SRE 的条件下，控制面复杂度的相对成本下降，但仍高于当前数据面成熟度，建议“保留但不再扩展”。更关键的是，这些复杂度目前主要加在控制面，而数据面最需要的原子写入、幂等、续跑、重试还没有落地，形成了“控制面越来越完整、数据面越来越危险”的倒挂。

另一个过度设计是**分片数 = chunk 数**：

- `ShardRanges.split` 按 `chunkSize` 切分（`ShardRanges.java:28-35`），30M 行 / 5k = 6000 个 shard；
- `shard-max-total=5000`（`task-management-service.yml:77`），方案 Q2 自己承认 6000 会越限；
- 每个 shard 一条 dispatch 消息、一条 checkpoint、多条 progress/results，单个任务就是数千条控制消息；
- 对 20 个并发任务，控制面消息量和 DB 聚合压力会被放大。

建议把“分片”定义为**并行单元**（例如 8~32 个/任务），把“chunk”定义为**事务单元**（5k~20k 行）；一个 shard 内跑多个 chunk，而不是“一个 chunk 一个 shard”。

### 4.2 关键能力遗漏

1. **安全**：凭据不落 Kafka、DB 加密、JWT 密钥治理、RBAC。补充件明确说“无条件先做”，但 Phase 1 实现仍把 `sourcePassword` 放进派工消息。
2. **数据正确性**：数值精度、日期/时区、geometry/SRID、MySQL→PG 类型映射、NULL/编码。H1 甚至在新 worker 的默认 JDBC 转换链里被重现。
3. **源端一致性**：没有快照/高水位线；`WHERE pk BETWEEN` 跨分片扫描无法保证并发写入下的“不漏不重”。
4. **幂等/PK/upsert**：被推迟到 Phase 2，但 Phase 1 已经允许 `newWorker=true`；没有 PK 的重试必然重复。
5. **失败处置**：`ErrorClassifier`、自动重试、DLT 生产端都不存在；Phase 1 的“失败处置”实际是“失败即终态”。
6. **多实例协调**：`TokenBucket` 是 JVM 本地 `Semaphore`，不是全局；多 orchestrator 实例或重启后会失准。
7. **HA/DR**：PG/Kafka/Redis 全单节点、RF=1；没有备份/恢复/切换预案。
8. **数据质量与对账**：没有源/目标行数校验和、分片级 checksum、抽样比对；G-4 只对计数，不对内容。
9. **目标表治理**：每个任务建一张新表，无清理/归档/血缘；M1 未进入任何阶段。
10. **性能基线**：没有修复后的 P95/吞吐/GC 基线，无法验证“P95 降 40%”。

### 4.3 分阶段迁移、灰度、回滚的可操作性

- **灰度不是按 dataSource 的**。方案说“按 `dataSourceId` 灰度”，但代码只有一个全局 `etl.routing.new-worker`（`RoutingProperties.java:24`），没有 dataSource 白名单/比例路由。实际是“全开/全关”。
- **`shadow-compare` 是死配置**。`RoutingProperties.shadowCompare` 只有 getter/setter，没有任何比对作业或报表实现；方案 §11.4 的三项比对没有落地。
- **旧 `TaskScheduler` 没有受 `newWorker` 门控**。`TaskScheduler.processNewTasks()` 仍每 30s 扫描 `status='new'` 并直接写 `processing/running/failed`（`TaskScheduler.java:54-95`）。`EtlTaskService.createTask()` 先保存 `new`，再建 run；如果建 run 失败，任务会停在 `new`，旧调度器仍会启动旧链路，造成新旧双跑。
- **回滚只“停新链路”，不处理在途 run**。`newWorker=false` 会停掉 pump 和新监听器，但 DB 里已有的 `CREATED/QUEUED/DISPATCHED/RUNNING` run 不会被旧链路接管；它们会成为孤儿。方案没有“停止新提交 → 排空/取消在途 run → 切回旧链路”的 runbook。
- **旧链路 completed 适配层可能被 G-4 挡住**。`TaskCompletedListener.routeToStateMachine()` 把旧 `completed` 转 `SUCCEEDED`，但旧链路没有 checkpoint；G-4 很容易失败，结果 run 停在原状态，最后由 sweeper 判 `NEEDS_ATTENTION`（`TaskCompletedListener.java:84-90`）。
- **Phase 2 的建表改造指向了错误的代码路径**。方案 §7.1/§13 说改 `TaskScheduler.generateCreateTableSql`，但新链路实际用的是 `JdbcSinkWriter.buildCreateTable`（`JdbcSinkWriter.java:116-121`）。只改旧 `TaskScheduler`，新链路目标表仍然没有 PK/唯一键。

---

## 5. P0 问题（阻断生产灰度）

### P0-1 `[设计+实现]` 派工泵只派首批 8 个分片，剩余 PENDING 永不派发

**位置/证据**

- `DispatchService.pump()` 只查询 `findQueuedForDispatch`（`DispatchService.java:89-92`），而该查询只返回 `status='QUEUED'`（`EtlTaskRunRepository.java:172-174`）。
- `dispatchOne()` 最多派 `min(pending, permitsPerRun=8)` 个分片（`DispatchService.java:109-117`），首片派出后立刻 `QUEUED → DISPATCHED`（`:127-132`）。
- run 进入 `DISPATCHED/RUNNING` 后，没有任何代码继续扫描它的 PENDING 分片；`findPendingShards` 只在 `dispatchOne` 内被调用。
- E2E 只有 3 个分片（`FullChainE2EIT.java:97`），单测也只覆盖 2/3 片（`DispatchServicePumpTest.java:80-97,114-133`），所以测试全绿掩盖了这个问题。

**影响**

- 30M 行 / 5000～6000 分片的任务，只有前 8 片会被执行；其余分片永远停在 PENDING。
- 如果 worker 有心跳，run 会停在 `RUNNING`，30 分钟后被 `StallAndCancelSweeper` 判 `NEEDS_ATTENTION`，而不是自动继续派工。
- 直接导致方案 §7.3 的 P95 降 40% 目标不可达，也让 `permitsPerRun=8` 和全局令牌桶失去意义。

**修改建议**

- 增加“补派”循环：对 `DISPATCHED/RUNNING` 且仍有 PENDING 分片的 run，按可用令牌继续派工；`QUEUED → DISPATCHED` 只发生一次。
- 或者把派工改成“按 run 持续拉取 PENDING + 令牌可用即派”，不要以 run 状态作为唯一入口。
- 增加 100/1000 分片的集成测试，断言最终没有 PENDING，且所有分片都收到 dispatch。

### P0-2 `[设计+实现]` checkpoint 不可续跑，数据与断点不在同一事务，重试重复不可见

**位置/证据**

- `ShardExecutor.commitChunk()` 先调用 `sink.writeBatch()`，再调用 `checkpointWriter.upsert()`（`ShardExecutor.java:106-115`）；两者使用不同连接、不同事务。
- `JdbcSinkWriter.writeBatch()` 自己 `setAutoCommit(false)`、`commit()`（`JdbcSinkWriter.java:58-76`）；`CheckpointWriter` 用 `JdbcTemplate` 自动提交（`CheckpointWriter.java:38-42`）。数据提交成功、checkpoint 写失败时，重试会重复写数据。
- `commitChunk()` 传给 checkpoint 的 `lastCursor` 是 `null`（`ShardExecutor.java:111-112`）；`CheckpointWriter` 用 `COALESCE` 保留旧值（`:21-30`），所以 chunk 级根本没有游标。
- `JdbcSourceConnector.open()` 只用 `pkFrom/pkTo`（`JdbcSourceConnector.java:101-117`），完全不读 `shard.resumeCursor()`；派工消息里的 `resumeCursor` 是死字段。
- `JdbcSinkWriter` 是普通 INSERT，目标表没有 PK/唯一键（`:110-121`）；`CheckpointWriter` 用 `GREATEST` 保证计数单调（`:24-27`），重复读/重复写后 `rows_in/rows_out` 看起来仍然平衡。

**影响**

- 进程在“目标写入已 commit、checkpoint 未写”之间崩溃 → 重试从 `pkFrom` 重读，目标表出现重复行；G-4 仍可能通过，因为计数被 `GREATEST` 掩盖。
- 失败分片无法从 `last_cursor` 续跑，“断点续传唯一真源”目前是**只写不读**。
- 没有 PK/upsert 时，任何重试/重放都会产生重复数据；Phase 1 的“失败恢复”不可用。

**落地说明（跨库场景）**

如果目标表与 `etl_task_checkpoint` 不在同一 PostgreSQL，不应为了“同事务”引入 XA/2PC；应使用“目标表幂等键 + 目标提交后写 checkpoint + 重放验证”达到等价正确性。只有同库部署时才使用本地同事务。评审中的“同事务”是正确性目标，不是强制 2PC。

**修改建议**

- 让目标写入与 checkpoint 共享同一个 `DataSource` + `TransactionTemplate`（或显式传入 `Connection`），保证“数据 + 断点”同事务提交。
- 每个 chunk 写 `last_pk`/`last_cursor`；`open()` 从 `resumeCursor` 之后开始（注意闭区间边界，避免重复或漏行）。
- Phase 1 就引入目标表 PK/`idempotency_key UNIQUE` + `ON CONFLICT`，不要等到 Phase 2；否则无法安全重试。
- 增加故障注入测试：在 sink commit 后、checkpoint 写入前 kill worker，重启后断言目标行数不重复、G-4 仍成立。

### P0-3 `[实现]` 派工/回报异步发送未确认，`ASSIGNED` 被当“在跑”，60s 回退实际失效

**位置/证据**

- `KafkaDispatchPublisher.publish()` 调用 `kafkaTemplate.send(topic, key, payload)` 后立即返回，只捕获序列化异常（`KafkaDispatchPublisher.java:33-40`）；broker 不可达/异步失败不会进入 catch。
- `DispatchService` 先 `publisher.publish(cmd)`，再 `checkpointRepository.markAssigned(...)`，再 T5（`DispatchService.java:122-132`）。消息可能已经“发出去但没到 broker”，而 DB 已标 ASSIGNED。
- `KafkaWorkerEventReporter.send()` 同样只调用异步 `send`，没有回调/`.get()`（`KafkaWorkerEventReporter.java:46-52`）。
- `SweeperPolicy.dispatchTimeout()` 只要 `quiesced=false` 就返回 `RECOVER_RUNNING`（`SweeperPolicy.java:28-35`）；`countActiveShards` 把 `ASSIGNED` 也算作活跃（`EtlTaskCheckpointRepository.java:49-52`）。因此“只发了消息、worker 从未领取”的 run 会被补登记为 `RUNNING`，而不是 `QUEUED` 重派。
- `DispatchTimeoutSweeper` 只做 T8/T9（`DispatchTimeoutSweeper.java:71-101`），没有“ASSIGNED-only 视为无 worker”的分支。

**影响**

- 派工消息丢失 → shard 停在 ASSIGNED，run 被误标 RUNNING，30 分钟后才进 NEEDS_ATTENTION；60s 自动回退的承诺不成立。
- 消息在 DB 事务回滚前已发出 → 后续可能重复派工；worker 没有 lease/幂等检查，可能双跑。
- results 发送失败不会被感知，run 只能等 30 分钟 stall 兜底。

**修改建议**

- Phase 1 至少使用 `send(...).get(timeout)` 或带回调的发送，失败时保持 PENDING/不标 ASSIGNED；更长远的方案是 outbox。
- `markAssigned` 应在发送确认之后，或与 outbox 插入同事务。
- T8 判定应区分 `ASSIGNED` 与 `RUNNING`：只有存在 `RUNNING` 分片或真实心跳时才允许 `RECOVER_RUNNING`；只有 `ASSIGNED` 时应回退 `QUEUED` 并重派。
- 增加“ASSIGNED-only、无心跳”的 sweeper 测试。

### P0-4 `[实现]` 逐行降级写入未提交，`rowsDropped` 混用，静默丢数仍可判 `SUCCEEDED`

**位置/证据**

- `JdbcSinkWriter.writeBatch()` 批量失败后 `rollback()`，进入 `writeRowByRow()`；后者执行 `executeUpdate()` 但**没有 commit**（`JdbcSinkWriter.java:79-96`）。PostgreSQL JDBC 在连接关闭时对活动事务执行回滚，逐行“成功”的行实际不会落库；即使某些驱动提交，也属于未定义行为。
- `ShardExecutor` 在转换器返回 `null` 时把行计入 `rowsDropped`（`ShardExecutor.java:74-76`），在 `JdbcSinkWriter` 行失败时也计入 `rowsDropped`（`:90-95`）。两种语义被混用。
- `ShardExecutor` 最终无条件标 `SUCCEEDED`（`:98-101`），不检查 `rowsDropped>0`。
- `GuardChain` 的 G-4 只检查 `rows_out + rows_dropped = rows_in`（`GuardChain.java:92-96`）；`ConvergencePolicy` 也允许 dropped 的 run 进 `SUCCEEDED`（`ConvergencePolicy.java:46-52`）。

**影响**

- 目标表缺行、但 run 显示 SUCCEEDED；这是最危险的“静默数据丢失”。
- 没有 DLT 记录，运维无法知道哪些行失败；`rowsDropped` 指标也无法区分“业务过滤”和“写入失败”。
- 与方案“DLT 非空即 NEEDS_ATTENTION / PARTIAL_SUCCESS”的目标直接矛盾。

**修改建议**

- `writeRowByRow` 必须显式 `commit()`；更好的做法是用 savepoint 或单行 `ON CONFLICT`，不要依赖 `close()`。
- 拆分计数器：`rows_filtered`（业务规则丢弃，可进成功等式）与 `rows_failed`（写入/解析失败，必须进 DLT，且阻止 SUCCEEDED）。
- `SUCCEEDED` 的守卫改为“无 failed 行、无未解决 DLT”；`rows_failed>0` 时只能 `PARTIAL_SUCCESS` 或 `NEEDS_ATTENTION`。
- 增加“批中一行坏数据”的集成测试，断言坏行进 DLT、好行提交、run 不判 SUCCEEDED。

### P0-5 `[设计+实现]` 失败处置三段式重试/DLT 没有落地，所有异常直接终态

**位置/证据**

- 全仓没有 `ErrorClassifier` 类/接口；只有 `ErrorClass` 枚举。
- `ShardExecutor` 捕获所有 `RuntimeException` 后直接写 `FAILED` + `errorClass=PERSISTENT`（`ShardExecutor.java:88-94`），没有分片内重试，也没有 T12。
- `ResultsListener` 只把聚合结果映射为 `SUCCEEDED/PARTIAL_SUCCESS/FAILED/NEEDS_ATTENTION`（`ResultsListener.java:134-156`），不会调用 `RUNNING → RETRYING`。
- worker 没有任何代码向 `etl.task.dlt` 生产消息；`DltListener` 只在测试里被投喂。
- 方案 §4 要求“瞬态/持续性/永久性”三段式（`etl-architecture-evolution-plan.md:107-112`），但 `orchestrator-design.md §7.4` 的 `ShardFailureHandler` 没有对应实现。

**影响**

- 网络抖动、锁等待、临时不可用等瞬态错误会被当成持久失败，直接 `FAILED/PARTIAL_SUCCESS`。
- 任务级退避 5m/15m/1h、`attempt_no` 自增、DLT 隔离全部是“文档能力”，不是运行能力。
- 方案 §7.3-D“从 checkpoint 恢复重跑成功率 ≥95%”无法验收。

**修改建议**

- Phase 1 就实现最小 `ErrorClassifier`（TRANSIENT/PERSISTENT/FATAL）+ 分片内重试 + T12/T21；或者用 Spring Batch 的 retry/skip/restart 承担这部分。
- worker 对 FATAL 行写 `etl_task_dlt` 并发布 DLT；`rows_failed>0` 必须进 NEEDS_ATTENTION。
- 增加瞬态/持久/永久三类错误的集成测试，断言状态迁移和 DLT 行数。

### P0-6 `[实现]` 安全项未闭环：明文凭据进 Kafka/DB，JWT 密钥硬编码

**位置/证据**

- `ShardDispatchCommand` 含 `sourceUsername/sourcePassword`（`ShardDispatchCommand.java:26-29`）；`ShardDispatchAssembler` 从 data-source 服务取出后原样放入派工消息（`ShardDispatchAssembler.java:72-77`）；worker 的 `ShardDispatch` 也接收这些字段（`ShardDispatch.java:11-12,26-29`）。
- `db/schema/02-create-tables.sql:10` 的 `data_source.password` 是明文 `VARCHAR`；`DataSource.java:14` 直接映射。
- `user-service.yml:29-30` 与 `gateway-service.yml:88-89` 使用同一个硬编码 JWT secret。
- 补充件明确把 J1/J2/J3 列为“立即（不依赖重构）”（`risk-addendum-and-tech-selection.md:154`）。

**影响**

- 源库口令会落在 Kafka topic、消费者日志、DLT、审计数据中；任何有 topic 读权限的人都能拿到生产库凭据。
- 这是安全/合规阻断项，不能因为“Phase 1 骨架妥协”而进入生产。

**修改建议**

- 派工消息只传 `dataSourceId`；worker 用短期凭据/服务身份向 data-source-service 换取连接信息，或使用 Vault/KMS 动态凭据。
- DB 中的口令加密（Jasypt/KMS），REST/Feign 脱敏；JWT secret 移到密钥管理并轮换。
- 在 Phase 1 出厂门槛中加入“Kafka 消息中无明文凭据”的自动化检查。

### P0-7 `[实现]` 数值精度与 PostGIS/GeoJSON 主场景仍会失败或被静默改写

**位置/证据**

- `NumberScaleTransformer` 对所有 `BigDecimal`/`Double` 统一 `setScale(2)`（`NumberScaleTransformer.java:39-45`）；JDBC profile 默认包含它（`TransformerFactory.java:51-54`）。PostgreSQL `numeric` 读出来就是 `BigDecimal`，金额、坐标、ID 都会被四舍五入到 2 位。这是补充件 H1 的原样重现。
- `GeoJsonToWktTransformer` 只支持 Point/LineString/Polygon/MultiPoint/MultiLineString（`GeoJsonToWktTransformer.java:56-69`），不支持 MultiPolygon/GeometryCollection；WKT 不带 SRID。
- `JdbcSinkWriter` 用 `ps.setObject` 直接写 WKT 字符串（`JdbcSinkWriter.java:61-64`），没有 `PGobject`/`ST_GeomFromText(?,4326)`；目标表若按旧逻辑建 `geometry(polygon,4326)`（`TaskScheduler.java:176-178`），SRID 0 的 WKT 会失败。
- 对文件/JSON 源，`ensureTargetTable` 按源列类型建表（`JdbcSinkWriter.java:116-121`），GeoJSON 字段很可能被建成 `text`，而不是 `geometry`；转换后的 WKT 只是文本，不是空间类型。

**影响**

- 金额/坐标/ID 静默失真；这是数据正确性 P0，不是“格式问题”。
- 地楼房权人/PostGIS 是项目主场景；MultiPolygon、SRID、geometry 类型映射不解决，Phase 2/3 的 GeoJSON 接入无法交付。

**修改建议**

- 取消默认全局 `setScale(2)`；改为按目标列/任务 DSL 配置精度，`BigDecimal` 原样保留。
- 用 `PGobject(type="geometry", value=wkt)` + `ST_GeomFromText(?,4326)` 或 `ST_SetSRID`；明确目标列 SRID/几何类型。
- 增加 GeoJSON 类型映射（text/geometry）、MultiPolygon/GeometryCollection、MySQL→PG 类型映射表；用真实 MySQL/PostGIS 样本跑通。

### P0-8 `[设计+实现]` `taskId` 作派工 key + worker 单线程消费，单任务分片被串行化

**位置/证据**

- `KafkaDispatchPublisher.publish()` 使用 `command.getTaskId()` 作为 key（`KafkaDispatchPublisher.java:36`）；orchestrator 设计也明确 outbox key=taskId（`orchestrator-design.md:433`）。
- 同一 task 的所有 shard 消息落同一 partition；`DispatchBatchListener` 是同步阻塞执行（`DispatchBatchListener.java:31-50`），worker 没有配置 `spring.kafka.listener.concurrency`，默认 1。
- 这正好重现补充件 I1 的 P0 问题：“taskId 分区键 = 单任务串行”（`risk-addendum-and-tech-selection.md:38`），而方案的目标之一就是解决 I1。
- `max.poll.records` 默认 500；一个 poll 内串行执行多个 shard，若总耗时超过 `max.poll.interval.ms`（默认 5min），会触发 rebalance，消息被另一个 worker 重投，可能双跑。

**影响**

- 单个大任务无法利用多 worker/多分区并行；`permitsPerRun=8` 只是“同时在跑”的假象，实际仍是一分区一消费者。
- 长分片可能触发 rebalance，叠加没有 lease/fencing，造成重复执行。
- “分片并行 + P95 降 40%”的核心收益不成立。

**修改建议**

- 派工 key 改为 `runId + shardId`（或 `shardId`），让不同 shard 分散到不同分区；同 shard 的重复消息由 lease 去重。
- worker 配置 `concurrency`、`max-poll-records=1`（或异步执行 + pause/resume），确保长分片不阻塞 poll。
- 增加“1 个任务 8 个分片、2 个 worker”的并行测试，断言总耗时接近 1/2，且没有重复写入。

---

## 6. P1 问题（高优先级，进入灰度前应关闭）

### P1-1 `[设计+实现]` results 丢失后没有收敛 sweeper，正确数据也可能永远不进 SUCCEEDED

- `ResultsListener` 只在收到 results 时聚合（`ResultsListener.java:126-129`）；没有定时重新评估 `RUNNING` run 的作业。
- `StallAndCancelSweeper` 在 30 分钟后把 `RUNNING` 判 `NEEDS_ATTENTION`（`StallAndCancelSweeper.java:70-85`），即使所有 checkpoint 已经 SUCCEEDED。
- T25 `/resume` 只把状态改回 `RUNNING`，没有立即触发收敛重算（`orchestrator-design.md:236`）。

**建议**：增加 `ConvergenceSweeper`，对无 PENDING/ASSIGNED/RUNNING 分片的 `RUNNING/DISPATCHED` run 定期重算；`/resume` 后立即评估；增加“丢弃最后一条 results”测试。

### P1-2 `[实现]` T31 重放校验链不幂等，CREATED run 可能永久卡住

- `RunValidator.validateAndQueue()` 先 `planWriter.writeShardPlan()` 插入 PENDING checkpoint，再连续 transition（`RunValidator.java:98-107`）。
- `RunPlanWriter.writeShardPlan()` 使用 `saveAll`（`RunPlanWriter.java:29-35`），没有 `ON CONFLICT`/先删后插。
- `AcceptanceSweeper` 对 `CREATED` 驻留 run 重放整条校验链（`AcceptanceSweeper.java:98-104`）；如果上一次计划已落库但 transition 失败，重放会撞 `ux_checkpoint_shard`（`db/schema/06-etl-task-checkpoint.sql:35`），然后每 30s 重复失败。

**建议**：改为幂等 upsert，或先按 `run_id` 删除再插入；增加“计划落库后崩溃再重放”测试。

### P1-3 `[设计+实现]` G-2/G-4/T19/G-7 守卫不完整

- `ConvergencePolicy` 在 `failed>=1` 时直接返回 `PARTIAL_SUCCESS`，没有断言 `succeeded+failed+skipped == shardTotal`（`ConvergencePolicy.java:54-59`）。
- `GuardChain` 的 G-2 只检查 `failed>=1`（`GuardChain.java:99-104`）。
- T19 `PARTIAL_SUCCESS→SUCCEEDED` 只检查行数平衡（`GuardChain.java:83-96`），没有要求 failed 分片已 SKIPPED、DLT 已 resolved。
- G-7 `DLT_CLEAN` 未实现（`GuardChain.java:21-22`）。

**建议**：G-2 增加总和校验；T19 增加 `failed==0` 且无未解决 DLT；实现 G-7 或明确 Phase 1 禁止相关 API。

### P1-4 `[设计]` 对账等式在“重复读/去重”场景下不成立

- 设计 §7.5/D-08 规定 `rows_out + rows_dropped = rows_in`，并排除 `rows_deduped`，理由是“续跑时 rows_in 从 last_cursor 之后累计”（`orchestrator-design.md:533-544`）。
- 实现并不从 last_cursor 续跑（P0-2），重试会从 pkFrom 重读；如果目标表有 upsert，重复行会计入 `rows_deduped`，不进入等式 → 等式左边少、右边多，run 被误判 `NEEDS_ATTENTION`。
- 如果没有 upsert，重复行会再次计入 `rows_out`，等式成立但数据重复。

**建议**：明确 `rows_in` 语义；等式包含 `rows_deduped` 或单独的 `rows_replayed`；用崩溃/重放场景验证。

### P1-5 `[实现]` 没有 lease/fencing；checkpoint 状态可被旧 worker 回退，T25 可能永远无法 resume

- `CheckpointWriter.upsert` 无条件 `status = EXCLUDED.status`（`CheckpointWriter.java:21-30`）；旧 attempt 的迟到写入可以把 `SUCCEEDED` 改回 `RUNNING`。
- `GuardChain` 的 `NEEDS_ATTENTION→RUNNING` 要求 quiesce（`GuardChain.java:30-36`）；但 T13 进入 NEEDS_ATTENTION 时可能仍有 RUNNING 分片（`StallAndCancelSweeper`/`DltListener`），没有机制把“已死但状态仍 RUNNING”的分片重置。
- 没有 `shard_attempt`/lease 条件写入；`shard_attempt` 字段从未被更新。

**建议**：引入 lease/fencing token；checkpoint 写入带 attempt/lease 条件；`/resume` 重置过期 lease 分片并写 audit；增加旧 worker 迟到写入测试。

### P1-6 `[实现]` 令牌桶是 JVM 本地 `Semaphore`，不是全局、不抗重启

- `TokenBucket` 用 `new Semaphore(capacity, true)`（`TokenBucket.java:21-24`）；没有 Redisson/DB 共享。
- `ResultsListener`/`DltListener` 在各自实例上 `releaseWithinCapacity`（`ResultsListener.java:95`、`DltListener.java:93`）；多实例部署时，pump 所在实例的信号量不会被释放。
- `tryAcquire(permits)` 是全有或全无（`DispatchService.java:109-114`）；有 3 个空闲许可、需要 8 个时直接失败，导致 THROTTLED 饥饿。

**建议**：用 Redisson 分布式信号量，或直接用 `etl_task_checkpoint` 的 `COUNT(*) WHERE status IN ('ASSIGNED','RUNNING')` 作为全局占用数；`tryAcquire` 改为尽量获取。

### P1-7 `[实现]` V-3 是空校验，目标表在 worker 每个分片重复建，类型映射缺失

- `RunValidator` 的 V-3 只检查 `targetTableName` 非空（`RunValidator.java:92-96`），没有验证目标库权限、schema、列类型、geometry、PK。
- `ShardExecutor` 每个分片都调用 `sink.ensureTargetTable`（`ShardExecutor.java:62`）；并发分片会同时执行 `CREATE TABLE IF NOT EXISTS`，PG 仍可能因 `pg_type` 唯一冲突失败。
- `JdbcSinkWriter.buildCreateTable` 直接使用源列 `typeName`（`JdbcSinkWriter.java:116-121`），MySQL 类型名未必是合法 PG 类型；没有 PK/idempotency。

**建议**：在 validate/T2 阶段一次性创建/验证目标表（加 advisory lock）；增加 MySQL→PG 类型映射、geometry/SRID、列名引用规则；把 V-3 纳入真实建表/权限用例。

### P1-8 `[实现]` 提交同步执行 `COUNT(*)` 和每分片 Feign，30M 表提交会阻塞

- `EtlTaskService.createTask()` 同步调用 `runValidator.validateAndQueue()`（`EtlTaskService.java:76-78`），HTTP 请求要等校验完成。
- `JdbcSourceConnector.planShards()` 执行 `SELECT MIN, MAX, COUNT(*)`（`JdbcSourceConnector.java:78`）；performance-analysis 已指出 1400 万行级表 COUNT 可达秒级~十几秒（`performance-analysis.md:51`）。
- `ShardDispatchAssembler.assemble()` 每个分片都 `findByTaskId` + Feign `getDataSourceById`（`ShardDispatchAssembler.java:47-82`），5000 分片就是 5000 次 Feign。

**建议**：提交改为“建 run 后异步 validate”，HTTP 立即返回 `202`；数据源按 run 解析一次并缓存；大表用估算行数。

### P1-9 `[实现]` 新旧链路并存/灰度/回滚不可操作

- `TaskScheduler` 没有 `routing.isNewWorker()` 门控（`TaskScheduler.java:54-68`）；`createTask` 先保存 `new` 再建 run，存在旧调度器抢跑窗口。
- 只有全局开关，没有 dataSource 白名单/比例；`shadowCompare` 无实现。
- 回滚只停新链路，不处理在途 `CREATED/QUEUED/DISPATCHED/RUNNING` run；旧 `TaskScheduler` 只扫 `new`。
- `TaskCompletedListener` 适配到状态机时 G-4 可能失败（`TaskCompletedListener.java:84-90`）。

**建议**：`TaskScheduler` 增加 `newWorker` 门控 + dataSource 白名单；`createTask` 用同一事务建 task+run；实现 shadow compare；增加回滚 runbook。

### P1-10 `[设计]` 分片数 = chunk 数导致控制面消息量过大

- `ShardRanges.split` 用 `chunkSize` 作为每个 shard 的键跨度（`ShardRanges.java:28-35`）；30M/5k = 6000 shard。
- 方案 Q2 自己承认 6000 会超过 `shard-max-total=5000`（`orchestrator-design.md:957`）。
- 每个 shard 一条 dispatch、一条 checkpoint、多条 progress/results；5000 shard/任务 × 20 任务 = 10 万级控制消息。

**建议**：分片数按“期望并行度”决定（如 8~32/任务），chunk 作为分片内事务单元；如果必须细粒度分片，至少合并 progress 为 run 级心跳。

### P1-11 `[实现]` worker 端口/配置/指标/消费参数未就绪

- `etl-worker/application.yml:28-29` 设 `server.port: 8086`，与 `user-service` 的 8086 冲突（`user-service.yml:2`）。
- `config-repo` 没有 `etl-worker.yml`；worker 不走 config-server 的统一配置。
- `prometheus.yml:13-19` 只抓 8081-8085，没有 8086；worker 没有自定义 Micrometer 指标/MDC。
- 没有 `spring.kafka.listener.concurrency`/`max-poll-records`/`max-poll-interval-ms` 配置；长分片可能 rebalance。

**建议**：分配独立端口，加入 config-repo 和 Prometheus target；配置 listener concurrency/max-poll；worker 增加指标和 MDC。

### P1-12 `[设计]` 源端一致性未定义：无快照/高水位线，MySQL 流式读取也未区分

- `JdbcSourceConnector.open()` 用 `WHERE pk BETWEEN ? AND ?`，没有事务隔离/快照/高水位线（`JdbcSourceConnector.java:101-117`）。
- `planShards` 的 MIN/MAX/COUNT 是提交时快照；后续 shard 扫描期间源表仍可写入，可能漏行或重复。
- `ps.setFetchSize(5000)` 对 MySQL 和 PG 一样（`:110`）；方案 §5.2 要求 MySQL 用 `Integer.MIN_VALUE` 流式，否则可能全量缓冲 OOM。

**建议**：明确源端一致性语义；需要正确性时使用 PG `pg_export_snapshot`/repeatable read 或 MySQL consistent snapshot + 高水位线；MySQL 使用流式 fetch；增加查询超时/取消。

### P1-13 `[实现+文档]` API/网关/可观测性契约未落地，验收 B/C 无法验证

- 方案 §9 的 `/progress`、`/runs`、`/failures`、`/cancel`、`/retry`、`/resume`、`/abort` 均未实现；`EtlTaskController` 只有 CRUD（`EtlTaskController.java:38-55`）。
- `POST /api/tasks` 返回旧 `EtlTask`，`status='new'` 且没有 `runId`（`EtlTaskService.java:63-79`），与 §9 的 `201 {taskId,runId,status:CREATED}` 不一致。
- 网关路由是 `/api/task/**`（`gateway-service-routes.yml:27-32`），而控制器是 `/api/tasks`；新 API 经网关会 404。data-source 路由 `/api/data-source/**` 与 `/api/data-sources` 同样不匹配。
- `OrchestratorMetrics` 只有 Counter，没有 `etl.run.state.count`、`etl.checkpoint.lag.seconds`、`etl.dlt.unresolved.size` 等 Gauge（`OrchestratorMetrics.java:22-74`）；`alerts.yml` 没有 orchestrator 告警组；方案 §7.3-B/C 无法验收。

**建议**：Phase 1 至少实现 `/progress`、`/cancel`、`/failures`；修正网关路由；补齐 Gauge 和 `etl-orchestrator-alerts`；nightly SQL 落成定时任务/告警。

---

## 7. P2 问题（中优先级）

### P2-1 `[文档]` 方案内部存在指标标签与阶段指向矛盾

- 方案 §6.1 说指标带 `taskId` 标签（`etl-architecture-evolution-plan.md:163`），orchestrator D-24 明确“taskId 不进指标标签”（`orchestrator-design.md:690`）；应按 D-24 修正方案。
- Phase 2 指向 `TaskScheduler.generateCreateTableSql`，但新链路实际用 `JdbcSinkWriter.buildCreateTable`；应改指向新链路。
- 方案 §8 的 DDL 清单缺 `09-etl-outbox.sql`，orchestrator §4.2 有；两处应统一。
- 方案 §8 说产出 `postgres_health.json`，`monitoring/grafana/dashboards` 实际没有该文件。
- `maxAttempts` 注释写“attempt_no 上限 = maxAttempts + 1”（`OrchestratorProperties.java:19`），但代码 G-5 允许的 attempt 数是 `maxAttempts`；语义应统一。

### P2-2 `[实现+文档]` `etl_task` 表/实体/前端契约存在漂移

- `db/schema/02-create-tables.sql:25-26` 是 `total_shards/completed_shards`，实体是 `totalRecords/completedRecords`（`EtlTask.java:18-19`）；Hibernate `ddl-auto:update` 会新增列，旧列保留。
- 新链路把 `etl_task.status` 投影成 `CREATED/QUEUED/...`，旧前端只识 `new/running/completed/failed`；Q6 承认但没有适配层。
- `EtlTaskStatus.fromLegacyOrName` 不识别 `processing`（`EtlTaskStatus.java:60-71`），旧数据读取可能抛异常。
- `etl_task` 的 `totalRecords/completedRecords` 没有从 run/checkpoint 投影，旧 UI 计数会停在 0。

### P2-3 `[实现]` audit/event/outbox 仍是“文档能力”

- `INV-9` 要求每次状态变更写 `etl_task_audit`（`orchestrator-design.md:269`），但 `OrchestratorService` 没有 audit 写入（`OrchestratorService.java:155-207`）。
- `EtlTaskTransitioned` 发布后没有 `@TransactionalEventListener` 监听器（全仓无该注解），事件实际被丢弃；`SpringDomainEventPublisher` 的“AFTER_COMMIT 投递 Kafka”没有实现。
- outbox 表/poller 不存在，Phase 1 的 dispatch/results 是直投；这与“事件原子提交”的核心承诺不一致。

### P2-4 `[实现]` 调度/计数细节不精确

- `findQueuedOlderThan` 用 `updated_at` 作为入队时间（`EtlTaskRunRepository.java:143-146`）；`markQueuedReason` 不更新 `updated_at`，饥饿计时从入队起算，可能提前/延后。
- `WorkerProperties.progressIntervalMs` 没有被使用；worker 每个 chunk 都发 progress（`ShardExecutor.java:113-114`），快源可能远超“≤1/5s”，慢源可能超过 60s 心跳超时。
- `StallAndCancelSweeper` 只用 `stall-fallback-threshold=30m`（`StallAndCancelSweeper.java:72`），没有实现“15×平均 shard 耗时”的动态阈值。
- `OrchestratorMetrics` 没有 `etl.sweeper.actions.total`、`etl.run.duration` 等 §10.1 指标。

### P2-5 `[实现]` HA/运维/契约基础仍薄弱

- `docker-compose.yml` 的 PG/Redis/Kafka 都是单节点、RF=1；没有备份/恢复/切换预案。
- 没有资源限制/requests；4C8G 假设未在 compose 中体现；worker 不在 compose 内。
- 没有 Tempo/Jaeger/OTLP 后端；Sleuth 只写日志 traceId。
- Kafka 消息仍是手工 JSON String，没有 Schema Registry/契约测试；`ShardDispatchCommand` 与 `ShardDispatch` 字段靠人工对齐，存在漂移风险。
- 目标表“一任务一表”，无清理/归档/血缘；M1 未进入阶段计划。

### P2-6 `[实现]` 其他代码质量问题

- `EtlTaskService.submitToStateMachine` 吞掉所有 RuntimeException（`EtlTaskService.java:84-91`），建 run 失败时 task 可能停在 `new`，没有可见告警。
- `TaskScheduler` 的 `rLock.tryLock()`/`unlock()` 没有检查持有者；`processNewTask` 直接写状态字面量，仍是旧模型。
- `TaskCompletedListener/TaskFailedListener` 各自 `new ObjectMapper()`，且 catch 所有异常只记日志；旧链路失败仍可能静默。
- `JdbcSourceConnector` 对表名/列名直接字符串拼接（`JdbcSourceConnector.java:78,104`），存在注入面；应加白名单/引用标识符。
- `JdbcSourceConnector.readSchema` 不处理 schema-qualified 表名、大小写、MySQL catalog；`planShards` 要求整型 PK，但方案声称无 PK 表可用 `TABLESAMPLE/ctid`（`etl-architecture-evolution-plan.md:138`），两者不一致。

---

## 8. 待确认问题

1. **源表在 ETL 期间是否允许并发写？** 如果不允许，需明确停机窗口；如果允许，必须定义快照/高水位线语义，否则“不漏不重”无法保证。
2. **峰值并发任务/分片数是多少？** 20 个任务是平均值还是峰值？每个源库允许多少并发连接？
3. **orchestrator/worker 是否多实例部署？** 如果多实例，令牌桶、lease、sweeper 必须分布式；如果单实例，需明确单点故障的接受度。
4. **orchestrator 与 worker 是否始终共享同一 PostgreSQL？** 拆分部署时 checkpoint 原子性、凭据、网络延迟都要重新设计。
5. **目标表是否永久保留？** 谁负责清理/归档/血缘？每个任务建新表的模式是否可接受？
6. **凭据治理方案是什么？** Vault/KMS/Jasypt 是否可用？是否允许 worker 通过 dataSourceId 动态取凭据？
7. **`PARTIAL_SUCCESS`/数据丢失的业务容忍度？** 谁有 `etl:task:forgive` 权限？宽恕后如何审计/追责？
8. **`NEEDS_ATTENTION` 的运营流程是什么？** 7 天保留、值班、升级、人工处置 SLA 是否确定？
9. **是否允许引入 Spring Batch/Temporal？** 团队是否具备维护能力？如果允许，是否愿意用它们替代自研 sweeper/retry/outbox？
10. **性能基线是什么？** 当前 P95、吞吐、GC、源库 CPU/连接数是多少？目标 P95/吞吐是多少？
11. **回滚时在途 run 如何处理？** 允许取消/放弃，还是必须跑完？回滚窗口多长？
12. **源类型/PK 类型矩阵是什么？** 是否必须支持无 PK、字符串 PK、复合 PK、分区表、视图？
13. **目标 schema/type mapping 契约是什么？** geometry 的 SRID/类型、numeric 精度、日期时区、MySQL→PG 类型映射由谁定义？
14. **PG/Kafka/Redis 的 HA/备份/恢复要求是什么？** 单节点是否可用于生产？RPO/RTO 是多少？
15. **是否接受消息契约无 Schema Registry？** 如果否，Phase 1 是否要引入 Avro/JSON Schema？

---

## 9. 建议的验收与验证清单

### 9.1 P0 出厂门槛（全部通过才允许 `newWorker=true`）

| # | 验证项 | 通过标准 |
|---|---|---|
| 1 | 全量派工 | 100/1000 分片任务，所有 PENDING 最终变为 ASSIGNED/SUCCEEDED；结束后 `COUNT(PENDING)=0` |
| 2 | 断点续跑 | 在 sink commit 后、checkpoint 写入前 kill worker；重启后从 last_cursor 续跑；目标行数 = 源行数，无重复 |
| 3 | 数据+断点原子 | 同库：同一事务提交；跨库：幂等写入 + checkpoint after commit + 重放验证；注入 checkpoint 写失败，目标表不重复/不丢数 |
| 4 | 幂等/重复投递 | 重复 dispatch/results、Kafka 重投、broker 重启；目标表无重复，状态不回退 |
| 5 | 失败处置 | 瞬态错误重试后成功；持久错误进 RETRYING/退避；FATAL 行进 DLT 并进 NEEDS_ATTENTION |
| 6 | 丢数可见 | 批中一行坏数据：好行提交、坏行进 DLT、run 不判 SUCCEEDED |
| 7 | 安全 | Kafka 消息中无明文口令；DB 口令加密；JWT secret 不在代码库；RBAC 覆盖 cancel/retry/forgive |
| 8 | 数值/空间正确性 | 金额/坐标/ID 不被默认四舍五入；GeoJSON Point/Line/Polygon/MultiPolygon + SRID 4326 正确入 PostGIS |
| 9 | 并发派工 | 1 个任务 8 分片、2 个 worker，分片并行执行；无 rebalance 双跑 |
| 10 | 多实例 | 2 个 orchestrator + 2 个 worker；令牌桶/lease 全局有效；无饥饿/超发 |
| 11 | 结果丢失 | 丢弃最后一条 results；run 仍能通过收敛 sweeper 进 SUCCEEDED |
| 12 | T31 重放 | 计划落库后崩溃再重放；不撞唯一键，run 能继续 |
| 13 | 源端一致性 | 源表并发写场景下，明确快照/高水位线语义，行数对账可解释 |
| 14 | 观测 | `etl.run.state.count`、`etl.checkpoint.lag.seconds`、`etl.dlt.unresolved.size` 可见；NEEDS_ATTENTION/DLT 告警能触发；日志含 taskId/runId/shardId/attemptNo |

### 9.2 性能/容量

- 修复前后各采一次基线：P95/P99 任务时长、吞吐（行/秒）、Full GC 次数/停顿、源库 CPU/连接数、Kafka lag。
- 3000 万行、20 并发任务、4C8G 节点压测；目标 P95 降幅 ≥40% 或给出可解释的替代指标。
- 5000 分片 vs 32 分片两种配置对比，证明控制面开销可接受。
- MySQL 源 1000 万行流式读取，验证不 OOM；PostGIS 目标验证 geometry 写入吞吐。

### 9.3 迁移/灰度/回滚

- 1 个 `dataSourceId` 灰度：新旧链路影子比对行数/校验和/终态/耗时。
- 25%→50%→100% 放量，每阶段观察 1 周；任一 P0 指标异常自动停流。
- 回滚演练：停止新提交 → 排空/取消在途 run → `newWorker=false` → 旧链路接管；验证没有孤儿 run、没有重复写入。
- 旧 Redis `task:status:*` 加 TTL；旧链路 completed/failed 与新 run 的投影一致性纳入 nightly。

### 9.4 测试与文档

- `mvn test` 全绿；`mvn -pl task-management-service -am test -Pit`（真 PG/Kafka/Redis）全绿且无 skip。
- 新增：>8 分片派工、ASSIGNED-only 超时、崩溃续跑、重复投递、坏行 DLT、T31 重放、结果丢失、多实例令牌桶、PostGIS/GeoJSON、MySQL 类型映射。
- nightly 四条一致性 SQL 落成定时任务，非空即告警；增加源/目标行数与 checksum 校验。
- 文档修正：Phase 1 完成度标记、Phase 2 建表代码路径、`taskId` 指标标签、09 DDL、`postgres_health.json`、`maxAttempts` 语义、gateway 路由前缀。

---

## 10. 最终建议

1. **保留架构方向，但重新排序。** 先把数据面做到“可证明正确”，再补状态机细节；不要继续在 12 态/4 sweeper 上增加复杂度。
2. **Phase 1 最小安全范围**：目标表 PK/幂等、数据+checkpoint 同事务、last_cursor 续跑、全量派工、ErrorClassifier+重试+DLT、凭据治理。这六项不完成，不开启 `newWorker=true`。
3. **做一次 2 周 spike 决策**：用同一份需求分别验证“自研 worker + Spring Batch”与“Temporal 编排”两条路线的数据面正确性和运维成本；如果自研路线不能在 2 周内跑通崩溃续跑/重复投递/全量派工，就切换到成熟框架，状态机文档保留为契约。
4. **灰度前先做故障注入，不要只跑 happy path。** 现有 3 分片/20 行的 E2E 只能证明“接线通了”，不能证明“30M 行可恢复”。
5. **把安全项从“Phase 2 以后”提前到 Phase 1 出厂门槛。** 明文凭据进 Kafka 是不可接受的。

如果只能给一个决策建议：**现在不要按原计划进入生产灰度；用 P0 清单做一次“数据面正确性 gate”，通过后再谈 Phase 2。** 这不是否定方案，而是避免把“状态机设计得很完整”误当成“ETL 已经可靠”。
