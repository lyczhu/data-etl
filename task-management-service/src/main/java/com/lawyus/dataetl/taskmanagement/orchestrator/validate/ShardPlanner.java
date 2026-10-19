package com.lawyus.dataetl.taskmanagement.orchestrator.validate;

import com.lawyus.dataetl.connector.spi.ConnectorRegistry;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.SourceConnector;
import com.lawyus.dataetl.connector.spi.model.ShardPlan;
import com.lawyus.dataetl.connector.spi.model.ShardParams;
import com.lawyus.dataetl.connector.spi.model.SourceSpec;
import com.lawyus.dataetl.taskmanagement.client.DataSource;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.DispatchProperties;
import org.springframework.stereotype.Component;

import java.util.Locale;

/**
 * 分片计划器（{@code orchestrator-design.md §7.1} V-2/V-4）：给定解析后的 {@link DataSource} 与源表，
 * 用统一 SPI（{@link ConnectorRegistry} → {@link SourceConnector#planShards}）按整型主键区间生成分片计划。
 *
 * <p>Phase 1 仅 JDBC（POSTGRES/MYSQL）实装；未注册类型由 {@code ConnectorRegistry} 快速失败为
 * {@code UnsupportedConnectorException}，被 {@link RunValidator} 归为 V-2 校验失败。
 */
@Component
public class ShardPlanner {

    private final ConnectorRegistry registry;
    private final DispatchProperties properties;

    public ShardPlanner(ConnectorRegistry orchestratorConnectorRegistry, DispatchProperties properties) {
        this.registry = orchestratorConnectorRegistry;
        this.properties = properties;
    }

    /** 生成 JDBC 分片计划（含每片 {@code pkFrom/pkTo} 闭区间与预估总行数）。 */
    public ShardPlan plan(DataSource ds, String sourceTable) {
        ConnectorType type = mapType(ds.getType());
        SourceConnector connector = registry.sourceFor(type);   // V-2：类型未注册即抛，校验失败
        SourceSpec spec = new SourceSpec(type, ds.getUrl(), ds.getUsername(),
                ds.getPassword(), ds.getDriverClassName(), sourceTable);
        ShardParams params = new ShardParams(properties.getShardChunkSize(), properties.getShardMaxTotal());
        return connector.planShards(spec, params);
    }

    /** 数据源类型 → SPI {@link ConnectorType}（Phase 1 仅 JDBC；未知回落 POSTGRES）。可见以便单测。 */
    static ConnectorType mapType(String dataSourceType) {
        if (dataSourceType == null) {
            return ConnectorType.POSTGRES;
        }
        String t = dataSourceType.toLowerCase(Locale.ROOT);
        if (t.contains("mysql") || t.contains("mariadb")) {
            return ConnectorType.MYSQL;
        }
        return ConnectorType.POSTGRES;
    }
}
