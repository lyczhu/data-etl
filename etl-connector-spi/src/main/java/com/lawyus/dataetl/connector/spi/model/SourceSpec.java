package com.lawyus.dataetl.connector.spi.model;

import com.lawyus.dataetl.connector.spi.ConnectorType;

/**
 * 连接/读取一个源（或写入一个目标）所需的定位信息。JDBC 用 url/user/pass/driver/table；
 * 非 JDBC 源用 {@code url} 承载 uri（如 {@code file:///...}、{@code mongodb://...}、{@code kafka://topic}）。
 */
public record SourceSpec(ConnectorType connectorType,
                         String url,
                         String username,
                         String password,
                         String driverClassName,
                         String table,
                         /** 查询超时秒数（WP-17）；<=0 表示不设置。 */
                         int queryTimeoutSeconds) {

    /** 兼容旧调用：不设置查询超时。 */
    public SourceSpec(ConnectorType connectorType, String url, String username, String password,
                      String driverClassName, String table) {
        this(connectorType, url, username, password, driverClassName, table, 0);
    }
}
