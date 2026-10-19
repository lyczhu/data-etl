package com.lawyus.dataetl.worker.pipeline;

import com.lawyus.dataetl.connector.spi.CloseableIterator;
import com.lawyus.dataetl.connector.spi.ConnectorRegistry;
import com.lawyus.dataetl.connector.spi.SinkWriter;
import com.lawyus.dataetl.connector.spi.SourceConnector;
import com.lawyus.dataetl.connector.spi.model.SinkResult;
import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.connector.spi.model.TableSchema;
import com.lawyus.dataetl.worker.checkpoint.CheckpointWriter;
import com.lawyus.dataetl.worker.config.WorkerProperties;
import com.lawyus.dataetl.worker.model.ShardDispatch;
import com.lawyus.dataetl.worker.model.ShardProgress;
import com.lawyus.dataetl.worker.model.ShardResultMessage;
import com.lawyus.dataetl.worker.report.WorkerEventReporter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 进程内 E→T→L 单分片执行器（方案 §2.1 / §5.2）：读源游标 → 按 chunk 转换 → 批写目标 → 提交 checkpoint →
 * 分片结束发 results。<b>不引 Kafka 到数据面</b>，只在控制面回报。
 *
 * <p>checkpoint 由本执行器直写（{@code worker} 是唯一写者，§4.1）；results 只是触发器，终局由 orchestrator 聚合重算（§7.5）。
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
    private final WorkerEventReporter reporter;
    private final WorkerProperties properties;
    private final RecordTransformer transformer;

    public ShardExecutor(ConnectorRegistry registry,
                         CheckpointWriter checkpointWriter,
                         WorkerEventReporter reporter,
                         WorkerProperties properties,
                         RecordTransformer transformer) {
        this.registry = registry;
        this.checkpointWriter = checkpointWriter;
        this.reporter = reporter;
        this.properties = properties;
        this.transformer = transformer;
    }

    public SinkResult execute(ShardDispatch d) {
        SourceConnector source = registry.sourceFor(d.connectorTypeEnum());
        SinkWriter sink = registry.sinkFor(d.connectorTypeEnum());
        TableSchema srcSchema = source.readSchema(d.sourceSpec());
        TableSchema tgtSchema = new TableSchema(d.getTargetTable(), srcSchema.columns());
        sink.ensureTargetTable(tgtSchema, d.targetSpec());

        long rowsIn = 0;
        SinkResult total = SinkResult.zero();
        int chunkSize = Math.max(1, properties.getChunkSize());

        try (CloseableIterator<SourceRecord> it = source.open(d.toShard(), d.sourceSpec())) {
            List<SourceRecord> chunk = new ArrayList<>(chunkSize);
            while (it.hasNext()) {
                SourceRecord rec = it.next();
                rowsIn++;
                SourceRecord transformed = transformer.apply(rec);
                if (transformed == null) {
                    total = total.combine(new SinkResult(0, 1, 0));   // 被丢弃的行
                } else {
                    chunk.add(transformed);
                }
                if (chunk.size() >= chunkSize) {
                    total = commitChunk(d, tgtSchema, sink, chunk, rowsIn, total);
                    chunk.clear();
                }
            }
            if (!chunk.isEmpty()) {
                total = commitChunk(d, tgtSchema, sink, chunk, rowsIn, total);
                chunk.clear();
            }
        } catch (RuntimeException e) {
            log.error("shard {} failed for run {}: {}", d.getShardId(), d.getRunId(), e.getMessage(), e);
            checkpointWriter.upsert(d.getTaskId(), d.getRunId(), d.getShardId(), SHARD_FAILED,
                    null, rowsIn, total.rowsOut(), total.rowsDropped(), total.rowsDeduped(),
                    "PERSISTENT", abbreviate(e.toString()));
            reportResult(d, SHARD_FAILED, rowsIn, total, "PERSISTENT", abbreviate(e.toString()));
            return total;
        }

        // 全部分片游标越界且提交成功 → SUCCEEDED；last_cursor 记本 shard 上界
        checkpointWriter.upsert(d.getTaskId(), d.getRunId(), d.getShardId(), SHARD_SUCCEEDED,
                d.getPkTo() == null ? null : String.valueOf(d.getPkTo()),
                rowsIn, total.rowsOut(), total.rowsDropped(), total.rowsDeduped(), null, null);
        reportResult(d, SHARD_SUCCEEDED, rowsIn, total, null, null);
        log.info("shard {} succeeded run={} rowsIn={} {}", d.getShardId(), d.getRunId(), rowsIn, total);
        return total;
    }

    private SinkResult commitChunk(ShardDispatch d, TableSchema tgtSchema, SinkWriter sink,
                                   List<SourceRecord> chunk, long rowsIn, SinkResult running) {
        SinkResult batchResult = sink.writeBatch(tgtSchema, chunk);
        SinkResult combined = running.combine(batchResult);
        // 数据 + 断点：每个 chunk 提交即 checkpoint（与目标写同侧，§4.1）
        checkpointWriter.upsert(d.getTaskId(), d.getRunId(), d.getShardId(), SHARD_RUNNING,
                null, rowsIn, combined.rowsOut(), combined.rowsDropped(), combined.rowsDeduped(), null, null);
        reporter.reportProgress(new ShardProgress(d.getTaskId(), d.getRunId(), d.getShardId(),
                rowsIn, combined.rowsOut(), System.currentTimeMillis()));
        return combined;
    }

    private void reportResult(ShardDispatch d, String status, long rowsIn, SinkResult total,
                              String errorClass, String errorDetail) {
        ShardResultMessage m = new ShardResultMessage();
        m.setTaskId(d.getTaskId());
        m.setRunId(d.getRunId());
        m.setShardId(d.getShardId());
        m.setStatus(status);
        m.setRowsIn(rowsIn);
        m.setRowsOut(total.rowsOut());
        m.setRowsDropped(total.rowsDropped());
        m.setRowsDeduped(total.rowsDeduped());
        m.setErrorClass(errorClass);
        m.setErrorDetail(errorDetail);
        reporter.reportResult(m);
    }

    private static String abbreviate(String s) {
        if (s == null) {
            return null;
        }
        return s.length() <= 2000 ? s : s.substring(0, 2000);
    }
}
