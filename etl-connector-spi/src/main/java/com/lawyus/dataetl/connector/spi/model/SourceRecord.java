package com.lawyus.dataetl.connector.spi.model;

import java.util.Collections;
import java.util.Map;

/**
 * 源读出的一行（进程内 pipeline 的流转单元，方案 §2.1 数据面不再穿越 Kafka）。
 * {@code pk} 为该行的主键值（JDBC 为 Long；其它类型可为 String/Object），供幂等键与日志使用。
 */
public record SourceRecord(String shardId, Object pk, Map<String, Object> columns) {

    public SourceRecord {
        columns = columns == null ? Collections.emptyMap() : Collections.unmodifiableMap(columns);
    }
}
