# Connector SPI 契约

> 模块：`etl-connector-spi`（纯 jar，非服务）。对应《ETL 架构演进方案》§2.1 / §5.2，落地痛点 E1（多源接入避免"每种源一个微服务"）。
> Phase 1 骨架实装 JDBC（POSTGRES / MYSQL）；MONGO / FILE_* / KAFKA_IOT 在 Phase 2/3 各加一个实现并注册即可。

## 分层

```
etl-connector-spi
└─ com.lawyus.dataetl.connector.spi
   ├─ ConnectorType              源/汇类型枚举
   ├─ SourceConnector           读侧 SPI：readSchema / planShards / open
   ├─ SinkWriter                写侧 SPI：ensureTargetTable / writeBatch
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
- `SinkResult{rowsOut, rowsDropped, rowsDeduped}`：对账等式 `rowsOut + rowsDropped = rowsIn`，`rowsDeduped` 不入等式（D-08）。

## JDBC 参考实现要点

- **分片**：`SELECT MIN/MAX/COUNT(pk)` → `ShardRanges.split(min,max,chunkSize,maxShards)`（纯函数、单测覆盖）产出互不重叠闭区间。
- **读**：`WHERE pk BETWEEN ? AND ? ORDER BY pk` + `setFetchSize` 流式游标（PG 需关 autoCommit）。
- **写**：整批 `executeBatch` 提交；整批失败降级逐行、坏行计 `rowsDropped`。幂等（`ON CONFLICT (idempotency_key) DO NOTHING` + PK）随 Phase 2 的目标表 DDL 一并强制。

## worker 侧用法（`etl-worker`）

`ShardExecutor` 从 `etl.task.dispatch.batch` 收一个 `Shard`，进程内跑 `SourceReader → RecordTransformer → SinkWriter`，
每 chunk 直写 `etl_task_checkpoint`（worker 是唯一写者，写权矩阵 §4.1），分片结束发 `etl.task.results`。
`ConnectorRegistry` 由 `ConnectorConfig` 用 worker 自身 `DataSource` 组装。

## 扩展一种新源（checklist）

1. `ConnectorType` 加枚举；
2. 实现 `SourceConnector`（+ 目标侧 `SinkWriter`）；
3. 在 `ConnectorConfig` 注册到 `ConnectorRegistry`；
4. 若引入新驱动，加 `etl-worker` 运行时依赖；
5. 补该 connector 的纯逻辑单测（分片/游标/映射）。
