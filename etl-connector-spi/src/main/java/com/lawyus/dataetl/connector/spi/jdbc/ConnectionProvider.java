package com.lawyus.dataetl.connector.spi.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * 目标库连接供给（{@link JdbcSinkWriter} 使用）。worker 侧用 Spring 的 {@code DataSource::getConnection} 实现；
 * 测试可用内存库或桩。抽出来避免 SPI 模块耦合具体连接池。
 */
@FunctionalInterface
public interface ConnectionProvider {
    Connection getConnection() throws SQLException;
}
