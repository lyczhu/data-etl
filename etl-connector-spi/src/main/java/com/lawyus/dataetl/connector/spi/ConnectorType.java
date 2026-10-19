package com.lawyus.dataetl.connector.spi;

/**
 * 数据源/汇类型（{@code etl-architecture-evolution-plan.md §7.3 E}）。
 *
 * <p>Phase 1 骨架仅实装 JDBC（{@link #POSTGRES}/{@link #MYSQL}）；其余类型已登记枚举，
 * 对应 connector 未注册时 {@code ConnectorRegistry} 抛 {@link UnsupportedConnectorException}，
 * 由 Mongo/File/IoT 实现分别在 Phase 2/3 补齐。
 */
public enum ConnectorType {
    POSTGRES,
    MYSQL,
    MONGO,
    FILE_JSON,
    FILE_CSV,
    FILE_GEOJSON,
    KAFKA_IOT;

    /** 当前构建是否已提供该类型的参考实现。 */
    public boolean implementedNow() {
        return this == POSTGRES || this == MYSQL;
    }
}
