package com.lawyus.dataetl.worker.pipeline;

import com.lawyus.dataetl.connector.spi.CloseableIterator;
import com.lawyus.dataetl.connector.spi.ConnectorRegistry;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.SinkWriter;
import com.lawyus.dataetl.connector.spi.SourceConnector;
import com.lawyus.dataetl.connector.spi.jdbc.JdbcSinkWriter;
import com.lawyus.dataetl.connector.spi.model.ColumnDef;
import com.lawyus.dataetl.connector.spi.model.FailedRow;
import com.lawyus.dataetl.connector.spi.model.Shard;
import com.lawyus.dataetl.connector.spi.model.SinkResult;
import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.connector.spi.model.SourceSpec;
import com.lawyus.dataetl.connector.spi.model.TableSchema;
import com.lawyus.dataetl.worker.checkpoint.CheckpointReader;
import com.lawyus.dataetl.worker.checkpoint.CheckpointWriter;
import com.lawyus.dataetl.worker.checkpoint.DltWriter;
import com.lawyus.dataetl.worker.checkpoint.ShardCheckpointState;
import com.lawyus.dataetl.worker.config.WorkerProperties;
import com.lawyus.dataetl.worker.datasource.DataSourceResolver;
import com.lawyus.dataetl.worker.datasource.ResolvedDataSource;
import com.lawyus.dataetl.worker.model.ShardDispatch;
import com.lawyus.dataetl.worker.model.ShardDltMessage;
import com.lawyus.dataetl.worker.model.ShardProgress;
import com.lawyus.dataetl.worker.model.ShardResultMessage;
import com.lawyus.dataetl.worker.report.WorkerEventReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import javax.sql.DataSource;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * 进程内 E→T→L 单分片执行器（方案 v2 WP-02/03/04/05/06）。
 *
 * <p>关键语义：
 * <ul>
 *   <li><b>续跑</b>：从 checkpoint 的 {@code last_cursor} 之后读；每个 chunk 提交后写游标；</li>
 *   <li><b>原子性</b>：同库时目标写入 + checkpoint + DLT 共享一个本地事务；跨库用幂等键 + checkpoint after commit；</li>
 *   <li><b>幂等</b>：每行带 {@code idempotency_key}，重放命中 {@code ON CONFLICT DO NOTHING}；</li>
 *   <li><b>丢数可见</b>：业务过滤计 {@code rows_filtered}，写入失败计 {@code rows_failed} 并落 DLT；</li>
 *   <li><b>失败处置</b>：{@link ErrorClassifier} 分类，TRANSIENT 退避重试，FATAL/PERSISTENT 耗尽后落 FAILED + DLT。</li>
 * </ul>
 */
@Component
public class ShardExecutor {

    private static final Logger log = LoggerFactory.getLogger(ShardExecutor.class);

    // 与 EtlShardStatus 对齐的字面量（worker 不依赖 task-management-service，故用常量）
    private static final String SHARD_RUNNING = "RUNNING";
    private static final String SHARD_SUCCEEDED = "SUCCEEDED";
    private static final String SHARD_FAILED = "FAILED";

    private final ConnectorRegistry registry;
    private final CheckpointWriter checkpointWriter;
    private final CheckpointReader checkpointReader;
    private final DltWriter dltWriter;
    private final WorkerEventReporter reporter;
    private final WorkerProperties properties;
    private final RecordTransformer transformer;
    private final DataSource dataSource;
    private final TransactionTemplate transactionTemplate;
    private final DataSourceResolver dataSourceResolver;
    /** WP-11：本 worker 进程的 lease 标识（pid@host-UUID）。 */
    private final String leaseOwner;

    public ShardExecutor(ConnectorRegistry registry,
                         CheckpointWriter checkpointWriter,
                         CheckpointReader checkpointReader,
                         DltWriter dltWriter,
                         WorkerEventReporter reporter,
                         WorkerProperties properties,
                         RecordTransformer transformer,
                         DataSource dataSource,
                         TransactionTemplate transactionTemplate,
                         DataSourceResolver dataSourceResolver) {
        this.registry = registry;
        this.checkpointWriter = checkpointWriter;
        this.checkpointReader = checkpointReader;
        this.dltWriter = dltWriter;
        this.reporter = reporter;
        this.properties = properties;
        this.transformer = transformer;
        this.dataSource = dataSource;
        this.transactionTemplate = transactionTemplate;
        this.dataSourceResolver = dataSourceResolver;
        this.leaseOwner = resolveLeaseOwner(properties);
    }

    private static String resolveLeaseOwner(WorkerProperties properties) {
        String configured = properties.getLeaseOwner();
        if (configured != null && !configured.isBlank()) {
            return configured;
        }
        // pid@host + 随机后缀：同一进程重启后也不会误认领旧 lease
        return ManagementFactory.getRuntimeMXBean().getName() + "-"
                + UUID.randomUUID().toString().substring(0, 8);
    }

    public SinkResult execute(ShardDispatch d) {
        int attempt = Math.max(1, d.getAttemptNo());
        // WP-11 lease/fencing：同一分片同一时刻只允许一个 worker 执行。lease 过期后可被接管；
        // 未拿到 lease 的重复投递直接 ack 跳过，由持锁 worker 完成并回报。
        int acquired = checkpointWriter.tryAcquireLease(
                d.getTaskId(), d.getRunId(), d.getShardId(), attempt, leaseOwner, properties.getLeaseSeconds());
        if (acquired == 0) {
            log.warn("shard {} lease not acquired run={} attempt={} owner={}",
                    d.getShardId(), d.getRunId(), attempt, leaseOwner);
            return SinkResult.zero();
        }

        ConnectorType type = d.connectorTypeEnum();
        SourceConnector source = registry.sourceFor(type);
        ResolvedDataSource sourceDs = dataSourceResolver.resolve(d.getDataSourceId());
        SourceSpec sourceSpec = sourceDs.toSourceSpec(type, d.getSourceTable(),
                properties.getQueryTimeoutSeconds());
        SinkWriter sink = sinkFor(d, type);

        TableSchema srcSchema = source.readSchema(sourceSpec);
        TableSchema tgtSchema = targetSchema(d, srcSchema);
        // WP-13：目标表已由 orchestrator V-3 在提交时一次性建好/校验，worker 不再每分片建表。

        int retries = 0;
        while (true) {
            ShardCheckpointState state = checkpointReader.load(d.getTaskId(), d.getRunId(), d.getShardId());
            Counters c = new Counters(state);
            String resume = state.hasCursor() ? state.lastCursor() : d.getResumeCursor();
            try {
                runOnce(d, source, sink, sourceSpec, tgtSchema, resume, attempt, c);
                if (c.rowsFailed > 0) {
                    // 好行已提交，坏行已落 DLT；分片判 FAILED，run 侧由 DLT 监听转 NEEDS_ATTENTION
                    finish(d, SHARD_FAILED, attempt, c, "FATAL", "rows_failed=" + c.rowsFailed);
                    reporter.reportDlt(new ShardDltMessage(d.getTaskId(), d.getRunId(), d.getShardId(),
                            "FATAL", "rows_failed=" + c.rowsFailed, c.rowsFailed));
                    reportResult(d, SHARD_FAILED, c, "FATAL", "rows_failed=" + c.rowsFailed);
                    return c.toSinkResult();
                }
                String finalCursor = d.getPkTo() == null ? c.lastCursor : String.valueOf(d.getPkTo());
                finish(d, SHARD_SUCCEEDED, attempt, c, null, null, finalCursor);
                reportResult(d, SHARD_SUCCEEDED, c, null, null);
                log.info("shard {} succeeded run={} rowsIn={} rowsOut={} deduped={}",
                        d.getShardId(), d.getRunId(), c.rowsIn, c.rowsOut, c.rowsDeduped);
                return c.toSinkResult();
            } catch (RuntimeException e) {
                ErrorClassifier.Kind kind = ErrorClassifier.classify(e);
                boolean retryable = kind == ErrorClassifier.Kind.TRANSIENT
                        && retries < Math.max(0, properties.getShardMaxRetries());
                if (retryable) {
                    retries++;
                    long backoff = Math.max(1, properties.getRetryBackoffMs()) * retries;
                    log.warn("shard {} transient failure run={} attempt={} retry {}/{} in {}ms: {}",
                            d.getShardId(), d.getRunId(), attempt, retries,
                            properties.getShardMaxRetries(), backoff, e.getMessage());
                    sleep(backoff);
                    continue;   // 重新读 checkpoint：从最后已提交 chunk 之后续跑
                }
                log.error("shard {} failed run={} class={}: {}", d.getShardId(), d.getRunId(),
                        kind, e.getMessage(), e);
                finish(d, SHARD_FAILED, attempt, c, kind.name(), abbreviate(e.toString()));
                try {
                    dltWriter.insertShardFailure(d.getTaskId(), d.getRunId(), d.getShardId(),
                            kind.name(), abbreviate(e.toString()));
                } catch (RuntimeException dltFailure) {
                    log.error("write shard DLT row failed run={} shard={}: {}",
                            d.getRunId(), d.getShardId(), dltFailure.getMessage());
                }
                reporter.reportDlt(new ShardDltMessage(d.getTaskId(), d.getRunId(), d.getShardId(),
                        kind.name(), abbreviate(e.getMessage()), 0));
                reportResult(d, SHARD_FAILED, c, kind.name(), abbreviate(e.toString()));
                return c.toSinkResult();
            }
        }
    }

    /** 读完整个分片；失败时抛出，由外层决定重试/落 DLT。 */
    private void runOnce(ShardDispatch d, SourceConnector source, SinkWriter sink,
                         SourceSpec sourceSpec, TableSchema tgtSchema, String resumeCursor,
                         int attempt, Counters c) {
        Shard shard = d.toShard(resumeCursor);
        int chunkSize = Math.max(1, properties.getChunkSize());
        long lastProgressAt = 0;
        try (CloseableIterator<SourceRecord> it = source.open(shard, sourceSpec)) {
            List<SourceRecord> chunk = new ArrayList<>(chunkSize);
            while (it.hasNext()) {
                SourceRecord rec = it.next();
                c.rowsIn++;
                SourceRecord transformed = transformer.apply(rec);
                if (transformed == null) {
                    c.rowsFiltered++;   // 业务规则过滤，可进成功等式
                    continue;
                }
                String key = IdempotencyKeys.of(d.getTaskId(), d.getSourceTable(), transformed.pk());
                chunk.add(IdempotencyKeys.withKey(transformed, key));
                if (chunk.size() >= chunkSize) {
                    SinkResult r = commitChunk(d, sink, tgtSchema, chunk, attempt, c);
                    chunk.clear();
                    if (r.rowsFailed() > 0) {
                        c.lastCursor = resumeBeforeFailure(r, c.lastCursor);
                        return;
                    }
                    lastProgressAt = maybeReportProgress(d, c, lastProgressAt, attempt);
                }
            }
            if (!chunk.isEmpty()) {
                SinkResult r = commitChunk(d, sink, tgtSchema, chunk, attempt, c);
                if (r.rowsFailed() > 0) {
                    c.lastCursor = resumeBeforeFailure(r, c.lastCursor);
                    return;
                }
                maybeReportProgress(d, c, lastProgressAt, attempt);
            }
        }
    }

    /**
     * 提交一个 chunk：同库时目标 + checkpoint + DLT 同事务；跨库时目标先幂等提交，
     * checkpoint 后写（重放安全）。计数只在事务提交成功后累加。
     */
    private SinkResult commitChunk(ShardDispatch d, SinkWriter sink, TableSchema tgtSchema,
                                   List<SourceRecord> chunk, int attempt, Counters c) {
        String lastPk = String.valueOf(chunk.get(chunk.size() - 1).pk());
        long rowsInSnapshot = c.rowsIn;
        long outSnapshot = c.rowsOut;
        long filteredSnapshot = c.rowsFiltered;
        long failedSnapshot = c.rowsFailed;
        long dedupedSnapshot = c.rowsDeduped;

        boolean sharedTx = sink.supportsExternalTransaction()
                && properties.isSameDatabaseTransaction()
                && d.getTargetDataSourceId() == null;   // 目标为 worker 本地库才可共享事务
        SinkResult result;
        if (sharedTx) {
            result = transactionTemplate.execute(status -> {
                try {
                    Connection conn = DataSourceUtils.getConnection(dataSource);
                    SinkResult sr = sink.writeBatch(conn, tgtSchema, chunk);
                    checkpointWriter.upsert(conn, d.getTaskId(), d.getRunId(), d.getShardId(), SHARD_RUNNING,
                            lastPk, lastPk, attempt,
                            rowsInSnapshot,
                            outSnapshot + sr.rowsOut(),
                            filteredSnapshot + sr.rowsFiltered(),
                            failedSnapshot + sr.rowsFailed(),
                            filteredSnapshot + sr.rowsFiltered() + failedSnapshot + sr.rowsFailed(),
                            dedupedSnapshot + sr.rowsDeduped(),
                            leaseOwner, null, null);
                    dltWriter.insertAll(conn, d.getTaskId(), d.getRunId(), d.getShardId(), sr.failures());
                    return sr;
                } catch (SQLException e) {
                    throw new RuntimeException(e);
                }
            });
        } else {
            result = sink.writeBatch(tgtSchema, chunk);
            checkpointWriter.upsert(d.getTaskId(), d.getRunId(), d.getShardId(), SHARD_RUNNING,
                    lastPk, lastPk, attempt,
                    rowsInSnapshot,
                    outSnapshot + result.rowsOut(),
                    filteredSnapshot + result.rowsFiltered(),
                    failedSnapshot + result.rowsFailed(),
                    filteredSnapshot + result.rowsFiltered() + failedSnapshot + result.rowsFailed(),
                    dedupedSnapshot + result.rowsDeduped(),
                    leaseOwner, null, null);
            dltWriter.insertAll(d.getTaskId(), d.getRunId(), d.getShardId(), result.failures());
        }
        c.rowsOut += result.rowsOut();
        c.rowsFiltered += result.rowsFiltered();
        c.rowsFailed += result.rowsFailed();
        c.rowsDeduped += result.rowsDeduped();
        c.failures.addAll(result.failures());
        c.lastPk = lastPk;
        c.lastCursor = lastPk;
        return result;
    }

    /** 坏行之前续跑：cursor = min(失败行主键) - 1；好行已写且幂等，重放不会重复。 */
    private static String resumeBeforeFailure(SinkResult result, String currentCursor) {
        long minFailed = Long.MAX_VALUE;
        for (FailedRow f : result.failures()) {
            try {
                minFailed = Math.min(minFailed, Long.parseLong(f.sourceKey()));
            } catch (RuntimeException ignore) {
                // 非数字主键：保持当前游标（无法精确回退）
            }
        }
        return minFailed == Long.MAX_VALUE ? currentCursor : String.valueOf(minFailed - 1);
    }

    private void finish(ShardDispatch d, String status, int attempt, Counters c,
                        String errorClass, String errorDetail) {
        finish(d, status, attempt, c, errorClass, errorDetail, c.lastCursor);
    }

    private void finish(ShardDispatch d, String status, int attempt, Counters c,
                        String errorClass, String errorDetail, String cursor) {
        checkpointWriter.upsert(d.getTaskId(), d.getRunId(), d.getShardId(), status,
                cursor, c.lastPk, attempt,
                c.rowsIn, c.rowsOut, c.rowsFiltered, c.rowsFailed, c.rowsDropped(), c.rowsDeduped,
                leaseOwner, errorClass, errorDetail);
    }

    private long maybeReportProgress(ShardDispatch d, Counters c, long lastProgressAt, int attempt) {
        long now = System.currentTimeMillis();
        long interval = Math.max(0, properties.getProgressIntervalMs());
        if (interval == 0 || now - lastProgressAt >= interval) {
            reporter.reportProgress(new ShardProgress(d.getTaskId(), d.getRunId(), d.getShardId(),
                    c.rowsIn, c.rowsOut, now));
            // WP-11：进度上报同时续约 lease；续约失败说明已被新 worker 接管，
            // 后续 checkpoint 写入会被 lease fencing 拒绝（此处不打断当前批，避免半批回滚）。
            if (properties.getLeaseSeconds() > 0) {
                int renewed = checkpointWriter.renewLease(d.getTaskId(), d.getRunId(), d.getShardId(),
                        leaseOwner, attempt,
                        new Timestamp(now + properties.getLeaseSeconds() * 1000L));
                if (renewed == 0) {
                    log.warn("shard {} lease lost run={} attempt={} owner={}",
                            d.getShardId(), d.getRunId(), attempt, leaseOwner);
                }
            }
            return now;
        }
        return lastProgressAt;
    }

    private SinkWriter sinkFor(ShardDispatch d, ConnectorType type) {
        if (d.getTargetDataSourceId() != null) {
            ResolvedDataSource target = dataSourceResolver.resolve(d.getTargetDataSourceId());
            return new JdbcSinkWriter(type, target::openConnection);
        }
        return registry.sinkFor(type);
    }

    private static TableSchema targetSchema(ShardDispatch d, TableSchema srcSchema) {
        List<ColumnDef> cols = new ArrayList<>(srcSchema.columns());
        boolean hasKey = cols.stream().anyMatch(c -> IdempotencyKeys.COLUMN.equalsIgnoreCase(c.name()));
        if (!hasKey) {
            cols.add(new ColumnDef(IdempotencyKeys.COLUMN, "varchar", Types.VARCHAR, 64, false));
        }
        return new TableSchema(d.getTargetTable(), cols);
    }

    private void reportResult(ShardDispatch d, String status, Counters c,
                              String errorClass, String errorDetail) {
        ShardResultMessage m = new ShardResultMessage();
        m.setTaskId(d.getTaskId());
        m.setRunId(d.getRunId());
        m.setShardId(d.getShardId());
        m.setStatus(status);
        m.setRowsIn(c.rowsIn);
        m.setRowsOut(c.rowsOut);
        m.setRowsDropped(c.rowsDropped());
        m.setRowsDeduped(c.rowsDeduped);
        m.setErrorClass(errorClass);
        m.setErrorDetail(errorDetail);
        reporter.reportResult(m);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("shard retry interrupted", e);
        }
    }

    private static String abbreviate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 2000 ? s : s.substring(0, 2000);
    }

    /** 跨 attempt 累计计数；旧库 rows_dropped 无 filtered/failed 明细时按过滤口径回填。 */
    private static final class Counters {
        private long rowsIn;
        private long rowsOut;
        private long rowsFiltered;
        private long rowsFailed;
        private long rowsDeduped;
        private String lastCursor;
        private String lastPk;
        private final List<FailedRow> failures = new ArrayList<>();

        private Counters(ShardCheckpointState s) {
            this.rowsIn = s.rowsIn();
            this.rowsOut = s.rowsOut();
            this.rowsFailed = s.rowsFailed();
            this.rowsFiltered = Math.max(s.rowsFiltered(), s.rowsDropped() - s.rowsFailed());
            this.rowsDeduped = s.rowsDeduped();
            this.lastCursor = s.lastCursor();
            this.lastPk = s.lastPk();
        }

        private long rowsDropped() {
            return rowsFiltered + rowsFailed;
        }

        private SinkResult toSinkResult() {
            return new SinkResult(rowsOut, rowsFiltered, rowsFailed, rowsDeduped, failures);
        }
    }
}
