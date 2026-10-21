package com.lawyus.dataetl.worker.pipeline;

import com.lawyus.dataetl.connector.spi.CloseableIterator;
import com.lawyus.dataetl.connector.spi.ConnectorRegistry;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.SinkWriter;
import com.lawyus.dataetl.connector.spi.SourceConnector;
import com.lawyus.dataetl.connector.spi.model.ColumnDef;
import com.lawyus.dataetl.connector.spi.model.FailedRow;
import com.lawyus.dataetl.connector.spi.model.Shard;
import com.lawyus.dataetl.connector.spi.model.ShardParams;
import com.lawyus.dataetl.connector.spi.model.ShardPlan;
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
import com.lawyus.dataetl.worker.model.ShardDltMessage;
import com.lawyus.dataetl.worker.model.ShardDispatch;
import com.lawyus.dataetl.worker.report.WorkerEventReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * 用假 connector + mock checkpoint/reporter 验证 ShardExecutor 的 v2 语义：
 * chunk 提交、断点续跑、幂等键、filtered/failed 分账、DLT、瞬态重试与同库同事务。
 * 纯逻辑，无需 DB/Kafka。
 */
class ShardExecutorTest {

    private CheckpointWriter checkpointWriter;
    private CheckpointReader checkpointReader;
    private DltWriter dltWriter;
    private WorkerEventReporter reporter;
    private WorkerProperties properties;
    private DataSourceResolver resolver;
    private FakeSink sink;

    @BeforeEach
    void setUp() {
        checkpointWriter = mock(CheckpointWriter.class);
        checkpointReader = mock(CheckpointReader.class);
        when(checkpointReader.load(anyString(), anyString(), anyString()))
                .thenReturn(ShardCheckpointState.empty());
        dltWriter = mock(DltWriter.class);
        reporter = mock(WorkerEventReporter.class);
        resolver = mock(DataSourceResolver.class);
        when(resolver.resolve(anyLong())).thenReturn(new ResolvedDataSource(
                1L, "postgresql", "jdbc:postgresql://localhost/etl_system",
                "postgres", "postgres", "org.postgresql.Driver"));

        properties = new WorkerProperties();
        properties.setChunkSize(3);
        properties.setProgressIntervalMs(0);
        properties.setSameDatabaseTransaction(false);
        properties.setLeaseOwner("worker-test");
        sink = new FakeSink();
        when(checkpointWriter.tryAcquireLease(anyString(), anyString(), anyString(), anyInt(),
                anyString(), anyLong())).thenReturn(1);
        when(checkpointWriter.renewLease(anyString(), anyString(), anyString(), anyString(),
                anyInt(), anyLong())).thenReturn(1);
    }

    private ShardExecutor executor(FakeSource source, RecordTransformer transformer) {
        ConnectorRegistry registry = new ConnectorRegistry().register(source).register(sink);
        return new ShardExecutor(registry, checkpointWriter, checkpointReader, dltWriter, reporter,
                properties, transformer, null, null, resolver);
    }

    @Test
    @DisplayName("8 行按 chunkSize=3 分块，聚合 rowsOut=8，收尾标 SUCCEEDED 并回报结果")
    void runsChunksAndSucceeds() {
        ShardExecutor executor = executor(new FakeSource(8), RecordTransformer.identity());

        SinkResult result = executor.execute(dispatch(7L));

        assertThat(result.rowsOut()).isEqualTo(8);
        assertThat(result.rowsFiltered()).isZero();
        assertThat(result.rowsFailed()).isZero();
        assertThat(sink.batchSizes).containsExactly(3, 3, 2);
        verify(reporter, atLeastOnce()).reportProgress(any());
        verify(checkpointWriter).upsert(eq("t-1"), eq("r-1"), eq("shard-0"), eq("SUCCEEDED"),
                eq("7"), eq("7"), eq(1), eq(8L), eq(8L), eq(0L), eq(0L), eq(0L), eq(0L),
                eq("worker-test"), isNull(), isNull());
    }

    @Test
    @DisplayName("transformer 返回 null 的行计入 rowsFiltered（不消耗目标写入配额）")
    void nullTransformCountsAsFiltered() {
        properties.setChunkSize(100);
        RecordTransformer dropEven = rec -> ((Long) rec.pk()) % 2 == 0 ? null : rec;
        ShardExecutor executor = executor(new FakeSource(5), dropEven);

        SinkResult result = executor.execute(dispatch(4L));

        assertThat(result.rowsOut()).isEqualTo(2);       // pk 0..4 → 只写 1,3
        assertThat(result.rowsFiltered()).isEqualTo(3);
        assertThat(result.rowsFailed()).isZero();
        assertThat(result.rowsDropped()).isEqualTo(3);
    }

    @Test
    @DisplayName("checkpoint 已有游标 → 从 last_cursor 之后续跑，不重复读已提交行")
    void resumesAfterCheckpointCursor() {
        when(checkpointReader.load("t-1", "r-1", "shard-0"))
                .thenReturn(new ShardCheckpointState("RUNNING", 1, "2", "2",
                        3, 3, 0, 0, 0, 0));
        ShardExecutor executor = executor(new FakeSource(8), RecordTransformer.identity());

        SinkResult result = executor.execute(dispatch(7L));

        assertThat(sink.batchSizes).containsExactly(3, 2);   // 只读 3..7
        assertThat(result.rowsOut()).isEqualTo(8);          // 3（已提交）+ 5（本轮）
    }

    @Test
    @DisplayName("写入前为每行追加 idempotency_key = sha256(taskId|sourceTable|pk)")
    void addsIdempotencyKey() {
        properties.setChunkSize(10);
        ShardExecutor executor = executor(new FakeSource(2), RecordTransformer.identity());

        executor.execute(dispatch(1L));

        assertThat(sink.batches).hasSize(1);
        for (SourceRecord record : sink.batches.get(0)) {
            Object key = record.columns().get(IdempotencyKeys.COLUMN);
            assertThat(key).isEqualTo(IdempotencyKeys.of("t-1", "demo_people", record.pk()));
        }
    }

    @Test
    @DisplayName("单行写入失败 → rowsFailed + DLT，分片 FAILED 且游标回退到失败行之前")
    void rowFailureGoesToDltAndFailsShard() {
        properties.setChunkSize(3);
        sink.failureForPk = 2L;
        ShardExecutor executor = executor(new FakeSource(4), RecordTransformer.identity());

        SinkResult result = executor.execute(dispatch(3L));

        assertThat(result.rowsOut()).isEqualTo(2);
        assertThat(result.rowsFailed()).isEqualTo(1);
        assertThat(result.failures()).hasSize(1);
        verify(dltWriter).insertAll(eq("t-1"), eq("r-1"), eq("shard-0"), any());
        verify(reporter).reportDlt(any(ShardDltMessage.class));
        verify(checkpointWriter).upsert(eq("t-1"), eq("r-1"), eq("shard-0"), eq("FAILED"),
                eq("1"), eq("2"), eq(1), eq(3L), eq(2L), eq(0L), eq(1L), eq(1L), eq(0L),
                eq("worker-test"), anyString(), anyString());
    }

    @Test
    @DisplayName("瞬态错误（SQLState 08*）按 shardMaxRetries 退避重试，第二次成功")
    void retriesTransientFailure() {
        properties.setShardMaxRetries(1);
        properties.setRetryBackoffMs(1);
        FakeSource source = new FakeSource(3);
        source.transientFailures = 1;
        ShardExecutor executor = executor(source, RecordTransformer.identity());

        SinkResult result = executor.execute(dispatch(2L));

        assertThat(source.openCalls).isEqualTo(2);
        assertThat(result.rowsOut()).isEqualTo(3);
        verify(checkpointWriter).upsert(eq("t-1"), eq("r-1"), eq("shard-0"), eq("SUCCEEDED"),
                any(), any(), eq(1), eq(3L), eq(3L), eq(0L), eq(0L), eq(0L), eq(0L),
                eq("worker-test"), isNull(), isNull());
    }

    @Test
    @DisplayName("同库同事务：sink/checkpoint/DLT 使用调用方 Connection")
    void usesSharedTransactionForLocalTarget() throws Exception {
        properties.setSameDatabaseTransaction(true);
        sink.externalTransaction = true;
        Connection connection = mock(Connection.class);
        SingleConnectionDataSource dataSource = new SingleConnectionDataSource(connection, true);
        TransactionTemplate tx = new TransactionTemplate(mock(PlatformTransactionManager.class)) {
            @Override
            public <T> T execute(TransactionCallback<T> action) {
                return action.doInTransaction(mock(TransactionStatus.class));
            }
        };
        ConnectorRegistry registry = new ConnectorRegistry()
                .register(new FakeSource(2)).register(sink);
        ShardExecutor executor = new ShardExecutor(registry, checkpointWriter, checkpointReader,
                dltWriter, reporter, properties, RecordTransformer.identity(),
                dataSource, tx, resolver);

        executor.execute(dispatch(1L));

        assertThat(sink.externalCalls).isEqualTo(1);
        verify(checkpointWriter).upsert(eq(sink.lastConnection), eq("t-1"), eq("r-1"), eq("shard-0"),
                eq("RUNNING"), eq("1"), eq("1"), eq(1), eq(2L), eq(2L), eq(0L), eq(0L), eq(0L), eq(0L),
                eq("worker-test"), isNull(), isNull());
        verify(dltWriter).insertAll(eq(sink.lastConnection), eq("t-1"), eq("r-1"), eq("shard-0"),
                eq(List.of()));
    }

    private static ShardDispatch dispatch(long pkTo) {
        ShardDispatch d = new ShardDispatch();
        d.setTaskId("t-1");
        d.setRunId("r-1");
        d.setShardId("shard-0");
        d.setAttemptNo(1);
        d.setPkFrom(0L);
        d.setPkTo(pkTo);
        d.setConnectorType(ConnectorType.POSTGRES.name());
        d.setDataSourceId(1L);
        d.setSourceTable("demo_people");
        d.setTargetTable("demo_people_t1");
        return d;
    }

    // ---- fakes ----

    private static TableSchema schema() {
        List<ColumnDef> cols = List.of(
                new ColumnDef("id", "int4", Types.INTEGER, 0, false),
                new ColumnDef("name", "varchar", Types.VARCHAR, 100, true));
        return new TableSchema("demo_people", cols);
    }

    private static final class FakeSource implements SourceConnector {
        private final int rows;
        int transientFailures;
        int openCalls;

        FakeSource(int rows) {
            this.rows = rows;
        }

        @Override public ConnectorType type() { return ConnectorType.POSTGRES; }
        @Override public TableSchema readSchema(SourceSpec spec) { return schema(); }
        @Override public ShardPlan planShards(SourceSpec spec, ShardParams params) {
            return new ShardPlan(List.of(), rows);
        }

        @Override
        public CloseableIterator<SourceRecord> open(Shard shard, SourceSpec spec) {
            openCalls++;
            if (transientFailures > 0) {
                transientFailures--;
                throw new RuntimeException(new SQLException("connection reset", "08006"));
            }
            long from = shard.resumeCursor() == null ? 0 : Long.parseLong(shard.resumeCursor()) + 1;
            List<SourceRecord> list = new ArrayList<>();
            for (long pk = from; pk < rows; pk++) {
                Map<String, Object> cols = new LinkedHashMap<>();
                cols.put("id", pk);
                cols.put("name", "n" + pk);
                list.add(new SourceRecord(shard.shardId(), pk, cols));
            }
            Iterator<SourceRecord> delegate = list.iterator();
            return new CloseableIterator<>() {
                @Override public boolean hasNext() { return delegate.hasNext(); }
                @Override public SourceRecord next() { return delegate.next(); }
                @Override public void close() { /* no-op */ }
            };
        }
    }

    private static class FakeSink implements SinkWriter {
        final List<Integer> batchSizes = new ArrayList<>();
        final List<List<SourceRecord>> batches = new ArrayList<>();
        boolean externalTransaction;
        int externalCalls;
        Connection lastConnection;
        Long failureForPk;

        @Override public ConnectorType type() { return ConnectorType.POSTGRES; }
        @Override public void ensureTargetTable(TableSchema schema, SourceSpec target) { /* no-op */ }
        @Override public boolean supportsExternalTransaction() { return externalTransaction; }

        @Override
        public SinkResult writeBatch(TableSchema schema, List<SourceRecord> batch) {
            batchSizes.add(batch.size());
            batches.add(List.copyOf(batch));
            if (failureForPk == null) {
                return new SinkResult(batch.size(), 0, 0, 0, List.of());
            }
            List<FailedRow> failures = new ArrayList<>();
            int ok = 0;
            for (SourceRecord record : batch) {
                if (failureForPk.equals(record.pk())) {
                    failures.add(new FailedRow("FATAL", "bad value", "{\"id\":" + record.pk() + "}",
                            String.valueOf(record.pk())));
                } else {
                    ok++;
                }
            }
            return new SinkResult(ok, 0, failures.size(), 0, failures);
        }

        @Override
        public SinkResult writeBatch(Connection conn, TableSchema schema, List<SourceRecord> batch) {
            externalCalls++;
            lastConnection = conn;
            return writeBatch(schema, batch);
        }
    }
}
