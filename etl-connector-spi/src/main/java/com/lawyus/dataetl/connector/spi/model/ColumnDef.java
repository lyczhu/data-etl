package com.lawyus.dataetl.connector.spi.model;

/**
 * 一列的元数据（{@link TableSchema} 的元素）。{@code jdbcType} 为 {@link java.sql.Types} 常量。
 */
public record ColumnDef(String name, String typeName, int jdbcType, int size, boolean nullable) {
}
