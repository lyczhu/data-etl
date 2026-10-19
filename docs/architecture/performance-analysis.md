# data-etl 性能瓶颈分析

> 状态：**分析记录，暂不改代码**（按要求仅记录 + 给出处理措施）。
> 生成方式：`risk-quality-reviewer` 视角（质量属性/性能）+ 真实运行证据。
> 关键前提：**系统当前没有可用的 QPS 指标与慢查询日志实测值**（见 §1），因此本报告的"吞吐/延迟"结论来自两类**真实证据**：① 各服务 `*/logs/gc.log`；② 代码与配置层面的结构性瓶颈。凡属推断处均显式标注。

## 1. 证据可得性（先说清楚能不能测到）

| 目标指标 | 现状 | 证据 |
|---|---|---|
| 每秒查询数 QPS / 吞吐 | ❌ 无实测 | Prometheus 仅配了抓取（`prometheus.yml` 15s），仓库无抓取快照；`metrics_examples.md:65` 声称已添加 `data-extraction-service/.../metrics/ExampleMetricService.java`，实际该类**不存在**（埋点未落地）。 |
| PostgreSQL 慢查询日志 | ❌ 未启用 | `docker-compose.yml` 的 `postgis` 容器未挂 `postgresql.conf`、未设 `log_min_duration_statement`、未载 `pg_stat_statements`；全仓无慢查询日志文件。 |
| JVM GC / 内存 | ✅ 有实测 | 4 个服务 `logs/gc.log`（G1、JDK25、堆 Xms=Xmx=512M）。见 §2。 |
| 运行期错误 | ✅ 有实测 | `gateway-service/logs/gateway-service-error.log`：`PSQLException: relation "gateway_route" does not exist`。**注**：这是早前 DB 动态路由方案被移除后的遗留报错（现行路由走 config+bus 动态刷新，见 `architecture-overview.md` §3.2），与性能无关。 |

**结论**：要给出"具体每秒查询数/慢查询 Top"，必须先补可观测性（§5）。在此之前只能给 GC 实测值 + 结构性瓶颈定位。

## 2. GC / 内存实测（真实数据）

从各 `gc.log` 统计（G1，堆上限均为 **512M**）：

| 服务 | 暂停次数 | **Full GC** | 最大暂停 | 平均暂停 | 判读 |
|---|---|---|---|---|---|
| **task-management-service** | 94 | **52** | **251.2 ms** | **94.6 ms** | 🔴 Full GC 风暴，内存严重不足 |
| data-transformation-service | 40 | 0 | 111.7 ms | 11.2 ms | 🟢 正常 |
| data-extraction-service | 50 | 0 | 102.5 ms | 7.6 ms | 🟢 正常 |
| data-source-service | 8 | 0 | 106.2 ms | 21.1 ms | 🟢 正常 |

**首要发现**：`task-management-service` 堆仅 512M 却承载 JPA + Kafka + Redisson + 被 storage 高频 Feign 调用，触发 52 次 Full GC、平均每次 94.6ms。它同时是"存储阶段每条记录被调用一次"的受害者（见 §3.1），GC 停顿会直接放大 ETL 尾延迟并可能引发 Feign 超时。

## 3. 结构性吞吐瓶颈（限制 QPS 上限的代码级原因）

沿数据流定位"最窄一环"：

### 3.1 存储阶段（🔴 全局瓶颈）
- **每条记录一次跨服务 Feign**：`DataStorageService.getTargetTableNameFromTaskService(taskId)` 每行都调 8082 取表名，**无缓存** → 百万行 = 百万次 HTTP RTT；而 8082 正在 Full GC 风暴 → 相互拖垮。（`docs/代码审查报告.md:255` 同结论：下游慢即全线阻塞）
- **"批量"退化单行**：`storeBatchDataToDatabase` 中 `batchArgs` 仅 `add(values)` 一行；整链路是**一行一条 Kafka 消息**（`DataExtractionService` 在 `while(rs.next())` 内逐条 `send`），`reWriteBatchedInserts=true` 形同虚设。
- **每行一次 Redis**：`updateCompletedRecords` 每条 `HINCRBY`。

### 3.2 抽取阶段
- 单线程顺序游标 `WHERE id >= ? LIMIT 1000`，逐批 `PreparedStatement`；生产端 `acks=all`、逐条 `send`。README 所称"分片并行 CompletableFuture"**代码未实现**。

### 3.3 转换阶段
- `DataTransformationService.processSingleRecord` 内 `send(...).get()` 同步阻塞；并发度受线程池限制（`ThreadPoolConfig`：corePool=CPU、queue=100、CallerRunsPolicy）。

### 3.4 网关（显式 QPS 天花板）
- 唯一与"每秒"直接相关的配置：`gateway-service-routes.yml` 各路由 `RequestRateLimiter.replenishRate`（每秒回填令牌）= data-source 10 / task 20 / extraction·transformation·storage 15 / user 10，维度为 `X-User-Id`/IP（`RateLimiterConfig`）。**经网关流量被封顶在每路由约 10~20 rps**；直连 8081–8086 不受此限。

## 4. 潜在慢查询（据 SQL 文本 + 表规模推断，非日志实测）

- `SELECT MIN(id), MAX(id), COUNT(*) FROM <table>`：对 `docs/系统说明.md` 所述 **1400 万行**级表，`COUNT(*)` 全表/全索引扫描，单次可达秒级~十几秒（抽取前置一次性开销）。
- **目标表建表未加主键/索引**：`TaskScheduler.generateCreateTableSql` 仅复制列类型不建 PK → 后续按 id 去重/翻页无索引可用；也使重复插入无法靠 PK 拦截。
- **逐行 INSERT**：每行一次网络往返 + 事务（见 §3.1）。

> 措辞强调：以上为"会慢"的强推断，缺慢查询日志无法给出"哪条 / 多少 ms"。

## 5. 处理措施

### 5.1 先补齐可观测性（否则永远拿不到你要的两个数）
- **QPS/吞吐**：
  - 查询 `rate(http_server_requests_seconds_count[1m])`（按 `application`/`uri`）——Micrometer 自动指标已在用。
  - Kafka 消费吞吐：`rate(kafka_consumer_records_consumed_total[1m])`。
  - 落地 `metrics_examples.md` 的 `etl.inbound.count` / `etl.processing.latency`（Timer，带 P50/P95），Grafana `etl_dashboard.json` 可直接消费。
- **慢查询**：为 postgis 容器加启动参数：
```yaml
postgres:
  command:
    -c log_min_duration_statement=100
    -c shared_preload_libraries=pg_stat_statements
    -c pg_stat_statements.track=all
```
  之后按 `pg_stat_statements.mean_exec_time` 排序取 Top-N 即真实慢查询。

### 5.2 优化优先级（Impact/Effort）

| 级别 | 措施 | 预期 | 成本 |
|---|---|---|---|
| **P0** | task-management 堆 512M→1G 并排查增长源 | 消除 Full GC 风暴、降尾延迟 | 低 |
| **P0** | storage 去每行 Feign：`taskId→targetTableName` 本地缓存（Caffeine/TTL） | 去掉 N×RTT，最大吞吐提升 | 中 |
| **P0** | storage/链路改**真批量**：一条 Kafka 消息携带多行，`batchUpdate` 真实批 | 数量级降 I/O 往返 | 中 |
| **P1** | 抽取并行分片 + 批量 `send`；转换去掉逐条 `.get()` | 提升 E/T 阶段并发 | 中 |
| **P1** | 大表 `COUNT(*)` 用估算(`reltuples`)或并行分片；目标表补 PK/索引 | 降前置耗时、护去重 | 中 |
| **P2** | 网关 `replenishRate` 按实测目标 QPS 校准 | 匹配容量 | 低 |

> 以上"处理措施"为**待办建议**，本报告不改代码。P0 三项是解锁吞吐的关键路径。

## 6.5 Phase 0 实施记录（2026-10）

按 `etl-architecture-evolution-plan.md §7 Phase 0`，以下已从"建议"转为"已落地"并通过 `mvn compile`（全 10 模块 BUILD SUCCESS）：

| 项 | 状态 | 落点 |
|---|---|---|
| P0 task-mgmt 堆 512M→1G | ✅ 已改 | `task-management-service/service.conf`、`start-service.bat` |
| P0 storage 去每行 Feign（目标表名缓存） | ✅ 已落地 | `DataStorageService.getCachedTargetTableName`（Caffeine TTL 5min） |
| P0 storage 真批插入 | ✅ 已落地 | storage `KafkaConfig.setBatchListener(true)` + `DataStorageService.storeBatch`（切片 500/批 + 单事务 executeBatch） |
| P1 可观测性（trace/指标/慢查询） | ✅ 部分 | Sleuth 接入 4 流水线服务 + `EtlMetrics`(etl.*)；`docker-compose.yml` 开 `pg_stat_statements`/慢查询；Grafana `etl_pipeline_hotpath.json`；`alerts.yml` 新增 5 条 |
| P1 抽取并行 / 大表 COUNT 优化 | ⏳ 未动 | 归入 Phase 1（分片派工在 worker 侧实现） |

> 注意：storage 消费语义由"单条"改为"批"，去重改为批内过滤 + 提交后打标记；非法消息由"抛错重试"改为"记录并跳过(poison guard)"以避免整批热循环——此为行为变更，Phase 1 将以 DLT 替代跳过。


## 6. 证据索引
`*/logs/gc.log`（G1/512M）· `gateway-service/logs/gateway-service-error.log` · `DataStorageService.java` · `DataExtractionService.java` · `DataTransformationService.java` · `config-repo/gateway-service-routes.yml` · `RateLimiterConfig.java` · `ThreadPoolConfig.java` · `docker-compose.yml` · `monitoring/metrics_examples.md` · `docs/代码审查报告.md:255` · `docs/系统说明.md`（数据规模）
