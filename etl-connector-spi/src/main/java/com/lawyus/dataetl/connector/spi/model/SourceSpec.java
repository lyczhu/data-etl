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
                         String table) {
}
