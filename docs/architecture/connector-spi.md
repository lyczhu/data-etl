# Connector SPI 契约

> 模块：`etl-connector-spi`（纯 jar，非服务）。对应《ETL 架构演进方案》§2.1 / §5.2，落地痛点 E1（多源接入避免"每种源一个微服务"）。
> Phase 1 骨架实装 JDBC（POSTGRES / MYSQL）；MONGO / FILE_* / KAFKA_IOT 在 Phase 2/3 各加一个实现并注册即可。

## 分层

```
etl-connector-spi
└─ com.lawyus.dataetl.connector.spi
   ├─ ConnectorType              源/汇类型枚举
   ├─ SourceConnector           读侧 SPI：readSchema / planShards / open
   ├─ SinkWriter                写侧 SPI：writeBatch（ensureTargetTable 仅供 V-3/迁移工具复用）
   ├─ ConnectorRegistry         类型→实现 注册表（未注册快速失败）
   ├─ CloseableIterator         可关闭游标（流式读，用完必关）
   ├─ model/                    SourceSpec / TableSchema / ColumnDef / Shard / ShardParams / ShardPlan / SourceRecord / SinkResult
   └─ jdbc/                     JdbcSourceConnector / JdbcSinkWriter / ShardRanges(纯分片) / ConnectionProvider
```

## 核心签名

```java
public interface SourceConnector {
    ConnectorType type();
    TableSchema readSchema(SourceSpec spec);
    ShardPlan planShards(SourceSpec spec, ShardParams params);
    CloseableIterator<SourceRecord> open(Shard shard, SourceSpec spec); // 调用方必须 close
}

public interface SinkWriter {
    ConnectorType type();
    void ensureTargetTable(TableSchema schema, SourceSpec target);
    SinkResult writeBatch(TableSchema schema, List<SourceRecord> batch);
}
```

- `Shard` 只带游标区间（JDBC：`pkFrom/pkTo` 闭区间；其它：`cursorFrom/cursorTo` + `resumeCursor`）——派工消息不带数据行（§5.2）。
  `pkTo` 在计划生成时即为**冻结的主键高水位**：run 期间源表新增行（pk > 高水位）不进入本次 run（WP-17 快照语义）。
- `SourceSpec` 含 `queryTimeoutSeconds`（0 = 不限制）：`planShards` 与 `open` 都按它设置语句超时；
  `etl-worker` 由 `WorkerProperties.query-timeout-seconds`（默认 600s）传入，避免慢查询永久占用分片。
- `SinkResult{rowsOut, rowsDropped, rowsDeduped}`：对账等式 `rowsOut + rowsFiltered + rowsFailed + rowsDeduped = rowsIn`（崩溃重放会再次读入 rowsIn，必须计入幂等命中）。

> 目标表建表**不在 worker 分片路径**：V-3 由 `TargetTableManager` 在目标库 advisory lock 下一次性建表并补
> `idempotency_key` 列 + UNIQUE 索引；`SinkWriter.ensureTargetTable` 仅为迁移/工具保留，不保证带幂等键。

## JDBC 参考实现要点

- **分片**：`SELECT MIN/MAX/COUNT(pk)` → `ShardRanges.split(min,max,chunkSize,maxShards)`（纯函数、单测覆盖）产出互不重叠闭区间；计划落库时冻结 `pkTo` 作为快照高水位。
- **读**：`WHERE pk BETWEEN ? AND ? ORDER BY pk` + `setFetchSize` 流式游标（PG 需关 autoCommit；MySQL 用 `Integer.MIN_VALUE` 触发流式 fetch）；语句超时取 `SourceSpec.queryTimeoutSeconds`。
- **写**：整批 `executeBatch` 提交；整批失败降级逐行、坏行计 `rowsDropped`。幂等靠目标表 `idempotency_key` 主键/唯一索引 + `ON CONFLICT (idempotency_key) DO NOTHING`，该约束由 V-3 保证。

## worker 侧用法（`etl-worker`）

`ShardExecutor` 从 `etl.task.dispatch.batch` 收一个 `Shard`，先以 `lease_owner/shard_attempt` 条件抢占该分片 checkpoint 的 lease
（被新 attempt 接管则直接放弃，不再写入），随后进程内跑 `SourceReader → RecordTransformer → SinkWriter`，
每 chunk 直写 `etl_task_checkpoint`（worker 是唯一写者，写权矩阵 §4.1）并续租，分片结束发 `etl.task.results`。
worker **不调用** `ensureTargetTable`：目标表缺失/不可写由 V-3 在提交期拦截（`TARGET_UNWRITABLE`）。
`ConnectorRegistry` 由 `ConnectorConfig` 用 worker 自身 `DataSource` 组装。

## 扩展一种新源（checklist）

1. `ConnectorType` 加枚举；
2. 实现 `SourceConnector`（+ 目标侧 `SinkWriter`）；
3. 在 `ConnectorConfig` 注册到 `ConnectorRegistry`；
4. 若引入新驱动，加 `etl-worker` 运行时依赖；
5. 补该 connector 的纯逻辑单测（分片/游标/映射）。
