package com.lawyus.dataetl.connector.spi.model;

import java.util.Collections;
import java.util.List;

/**
 * 表结构快照：源读与目标建表共用（{@code orchestrator-design.md} §5.2 / 方案 §5）。
 */
public record TableSchema(String table, List<ColumnDef> columns) {

    public TableSchema {
        columns = columns == null ? Collections.emptyList() : Collections.unmodifiableList(columns);
    }

    /** 主键列名（JDBC 分片按整型主键游标；无主键时为空，回退 TABLESAMPLE/ctid 由实现决定）。 */
    public String primaryKey() {
        return columns.isEmpty() ? null : columns.get(0).name();
    }
}
