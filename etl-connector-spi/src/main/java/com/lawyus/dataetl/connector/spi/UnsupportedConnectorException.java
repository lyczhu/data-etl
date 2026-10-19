package com.lawyus.dataetl.connector.spi;

/**
 * 请求了未实装的 {@link ConnectorType}（或类型无对应 connector）时抛出。
 * 让 worker 在计划阶段就快速失败，而不是运行到一半才 NPE。
 */
public class UnsupportedConnectorException extends RuntimeException {

    public UnsupportedConnectorException(String message) {
        super(message);
    }
}
