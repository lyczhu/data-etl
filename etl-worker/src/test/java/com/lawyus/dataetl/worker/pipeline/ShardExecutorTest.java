package com.lawyus.dataetl.worker.pipeline;

import com.lawyus.dataetl.connector.spi.CloseableIterator;
import com.lawyus.dataetl.connector.spi.ConnectorRegistry;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.SinkWriter;
import com.lawyus.dataetl.connector.spi.SourceConnector;
import com.lawyus.dataetl.connector.spi.model.ColumnDef;
import com.lawyus.dataetl.connector.spi.model.SinkResult;
import com.lawyus.dataetl.connector.spi.model.Shard;
import com.lawyus.dataetl.connector.spi.model.ShardParams;
import com.lawyus.dataetl.connector.spi.model.ShardPlan;
import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.connector.spi.model.SourceSpec;
import com.lawyus.dataetl.connector.spi.model.TableSchema;
import com.lawyus.dataetl.worker.checkpoint.CheckpointWriter;
import com.lawyus.dataetl.worker.config.WorkerProperties;
import com.lawyus.dataetl.worker.model.ShardDispatch;
import com.lawyus.dataetl.worker.report.WorkerEventReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.verify;

/**
 * 用假 connector + mock checkpoint/reporter 验证 ShardExecutor 的分块聚合、丢弃计数、SUCCEEDED 收尾。纯逻辑，无需 DB/Kafka。
 */
class ShardExecutorTest {

    private CheckpointWriter checkpointWriter;
    private WorkerEventReporter reporter;
    private WorkerProperties properties;
    private FakeSink sink;

    @BeforeEach
    void setUp() {
        checkpointWriter = mock(CheckpointWriter.class);
        reporter = mock(WorkerEventReporter.class);
        properties = new WorkerProperties();
        properties.setChunkSize(3);
        sink = new FakeSink();
    }

    @Test
    @DisplayName("8 行按 chunkSize=3 分块，聚合 rowsOut=8，收尾标 SUCCEEDED 并回报结果")
    void runsChunksAndSucceeds() {
        ConnectorRegistry registry = new ConnectorRegistry()
                .register(new FakeSource(8))
                .register(sink);
        ShardExecutor executor = new ShardExecutor(registry, checkpointWriter, reporter, properties, RecordTransformer.identity());

        SinkResult result = executor.execute(dispatch());

        assertThat(result.rowsOut()).isEqualTo(8);
        assertThat(result.rowsDropped()).isZero();
        assertThat(sink.batchSizes).containsExactly(3, 3, 2);          // 3+3+2
        verify(reporter, atLeastOnce()).reportProgress(any());
        // 收尾 checkpoint 与 results 均记 SUCCEEDED
        verify(checkpointWriter).upsert(eq("t-1"), eq("r-1"), eq("shard-0"), eq("SUCCEEDED"),
                any(), eq(8L), eq(8L), eq(0L), eq(0L), eq(null), eq(null));
    }

    @Test
    @DisplayName("transformer 返回 null 的行计入 dropped、不入 chunk")
    void nullTransformCountsAsDropped() {
        properties.setChunkSize(100);
        ConnectorRegistry registry = new ConnectorRegistry()
                .register(new FakeSource(5))
                .register(sink);
        RecordTransformer dropEven = rec -> ((Long) rec.pk()) % 2 == 0 ? null : rec;
        ShardExecutor executor = new ShardExecutor(registry, checkpointWriter, reporter, properties, dropEven);

        SinkResult result = executor.execute(dispatch());

        assertThat(result.rowsOut()).isEqualTo(2);  // pk 0..4，偶数(0,2,4)被丢 → 写 1,3
        assertThat(result.rowsDropped()).isEqualTo(3);
    }

    private static ShardDispatch dispatch() {
        ShardDispatch d = new ShardDispatch();
        d.setTaskId("t-1");
        d.setRunId("r-1");
        d.setShardId("shard-0");
        d.setPkFrom(0L);
        d.setPkTo(4L);
        d.setConnectorType(ConnectorType.POSTGRES.name());
        d.setSourceUrl("jdbc:postgresql://localhost/etl_system");
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
        FakeSource(int rows) { this.rows = rows; }

        @Override public ConnectorType type() { return ConnectorType.POSTGRES; }
        @Override public TableSchema readSchema(SourceSpec spec) { return schema(); }
        @Override public ShardPlan planShards(SourceSpec spec, ShardParams params) { return new ShardPlan(List.of(), rows); }

        @Override
        public CloseableIterator<SourceRecord> open(Shard shard, SourceSpec spec) {
            List<SourceRecord> list = new ArrayList<>();
            for (int i = 0; i < rows; i++) {
                Map<String, Object> cols = new LinkedHashMap<>();
                cols.put("id", (long) i);
                cols.put("name", "n" + i);
                list.add(new SourceRecord(shard.shardId(), (long) i, cols));
            }
            Iterator<SourceRecord> delegate = list.iterator();
            return new CloseableIterator<>() {
                @Override public boolean hasNext() { return delegate.hasNext(); }
                @Override public SourceRecord next() { return delegate.next(); }
                @Override public void close() { /* no-op */ }
            };
        }
    }

    private static final class FakeSink implements SinkWriter {
        final List<Integer> batchSizes = new ArrayList<>();

        @Override public ConnectorType type() { return ConnectorType.POSTGRES; }
        @Override public void ensureTargetTable(TableSchema schema, SourceSpec target) { /* no-op */ }

        @Override
        public SinkResult writeBatch(TableSchema schema, List<SourceRecord> batch) {
            batchSizes.add(batch.size());
            return new SinkResult(batch.size(), 0, 0);
        }
    }
}
