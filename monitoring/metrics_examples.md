# 自定义 Micrometer 指标示例

下面是一些在各服务中可复用的 Micrometer 指标模式，复制到各服务的合适位置（例如 `metrics` 包或服务的 `@Service` 类中）。

1) 计数器（接入总数 / 成功数 / 失败数）

```java
private final Counter inboundCounter = Counter.builder("etl.inbound.count")
    .description("Number of inbound records")
    .register(meterRegistry);

private final Counter successCounter = Counter.builder("etl.inbound.success.count")
    .description("Number of successfully processed records")
    .register(meterRegistry);

// 使用示例
inboundCounter.increment(recordsCount);
successCounter.increment(successCount);
```

2) 处理延迟（Timer）

```java
private final Timer processingTimer = Timer.builder("etl.processing.latency")
    .description("Processing latency")
    .publishPercentiles(0.5, 0.95) // 可选: 中位数与95百分位
    .register(meterRegistry);

// 使用示例
processingTimer.record(() -> {
    // 处理逻辑
});
```

3) 吞吐量 / 批量大小（DistributionSummary）

```java
private final DistributionSummary batchSizeSummary = DistributionSummary.builder("etl.batch.size")
    .description("Batch sizes processed")
    .register(meterRegistry);

// 使用示例
batchSizeSummary.record(batchSize);
```

4) 成功率（通过 Counter 结合 rate() 计算）

- 在 Prometheus 中可使用 `rate(etl.inbound.success.count[5m]) / rate(etl.inbound.count[5m])` 计算成功率。

5) 自定义标签（例如按数据源、任务ID区分）

```java
Counter.builder("etl.inbound.count")
    .tags("datasource", datasourceId)
    .register(meterRegistry)
    .increment();
```

6) 监控 HTTP 接口/数据库调用等

- Micrometer 与 Spring Boot 的自动 Metric（`http_server_requests_seconds`、`jdbc.connections.active` 等）配合使用即可。

把这些示例复制到以下服务的 `src/main/java/.../metrics` 包中，并注入 `MeterRegistry` 或直接在构造函数中创建指标。示例实现已添加到 `data-extraction-service`:

- `data-extraction-service/src/main/java/com/lawyus/dataetl/dataextraction/metrics/ExampleMetricService.java`

7) Phase 2 治理面指标（已实现，Prometheus 名称为下划线形式）

| Micrometer 名称 | 类型 | 来源 | 含义 |
|---|---|---|---|
| `etl.audit.write.failures` | counter | `AuditWriter` | 审计写失败次数（迁移已回滚） |
| `etl.audit.cleanup.deleted` | counter | `AuditRetentionSweeper` | 保留期清理行数 |
| `etl.outbox.pending` | gauge | `OutboxPoller` | 待投递行数 |
| `etl.outbox.oldest.pending.age.seconds` | gauge | `OutboxPoller` | 最老待投递行年龄 |
| `etl.outbox.sent` | counter | `OutboxPoller` | 成功投递行数 |
| `etl.outbox.send.failures` | counter | `OutboxPoller` | 投递失败次数 |
| `etl.outbox.cleaned` | counter | `OutboxPoller` | 已发送行清理数 |
| `etl.reconciliation.evaluations` | counter(tag `result`) | `ReconciliationService` | 对账结果 PASS/MISMATCH/ERROR |
| `etl.target.table.count` | gauge(tag `state`) | `TargetTableRegistry` | 目标表登记数 |