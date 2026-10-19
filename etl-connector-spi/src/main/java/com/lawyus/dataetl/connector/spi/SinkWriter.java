package com.lawyus.dataetl.connector.spi;

import com.lawyus.dataetl.connector.spi.model.SinkResult;
import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.connector.spi.model.SourceSpec;
import com.lawyus.dataetl.connector.spi.model.TableSchema;

import java.util.List;

/**
 * 统一 Sink SPI（方案 §2.1）：目标侧建表 + 幂等批写。
 *
 * <p>幂等约定（§4/§7.5）：目标表应带 {@code idempotency_key} 唯一约束，实现用 {@code INSERT ... ON CONFLICT DO NOTHING}
 * 返回 {@link SinkResult} 的 {@code rowsOut/rowsDeduped}。Phase 2 起 {@code ensureTargetTable} 强制加 PK +
 * {@code idempotency_key UNIQUE}；本骨架先支持普通批写。
 */
public interface SinkWriter {

    ConnectorType type();

    /** 目标表不存在则按源结构建表（best-effort；Phase 2 补 PK/唯一索引）。 */
    void ensureTargetTable(TableSchema schema, SourceSpec target);

    /**
     * 写入一批（同一 chunk）。实现应：整批 {@code executeBatch}，
     * 单行致命错误降级为逐行、计入 {@code rowsDropped}（其余照常提交）。
     */
    SinkResult writeBatch(TableSchema schema, List<SourceRecord> batch);
}
