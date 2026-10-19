package com.lawyus.dataetl.worker.datasource;

import com.lawyus.dataetl.connector.spi.model.SourceSpec;
import com.lawyus.dataetl.connector.spi.ConnectorType;

/**
 * 解析后的数据源连接信息（仅存在于 worker 内存，不进入 Kafka 消息/日志）。
 */
public record ResolvedDataSource(Long id,
                                 String type,
                                 String url,
                                 String username,
                                 String password,
                                 String driverClassName) {

    public SourceSpec toSourceSpec(ConnectorType connectorType, String table) {
        return new SourceSpec(connectorType, url, username, password, driverClassName, table);
    }

    public java.sql.Connection openConnection() {
        try {
            if (driverClassName != null && !driverClassName.isBlank()) {
                Class.forName(driverClassName);
            }
            return java.sql.DriverManager.getConnection(url, username, password);
        } catch (Exception e) {
            throw new RuntimeException("open target connection failed: " + e.getMessage(), e);
        }
    }
}
