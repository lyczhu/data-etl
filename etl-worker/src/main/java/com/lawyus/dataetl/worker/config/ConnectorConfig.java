package com.lawyus.dataetl.worker.config;

import com.lawyus.dataetl.connector.spi.ConnectorRegistry;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.jdbc.ConnectionProvider;
import com.lawyus.dataetl.connector.spi.jdbc.JdbcSinkWriter;
import com.lawyus.dataetl.connector.spi.jdbc.JdbcSourceConnector;
import com.lawyus.dataetl.worker.pipeline.DataProfile;
import com.lawyus.dataetl.worker.pipeline.RecordTransformer;
import com.lawyus.dataetl.worker.pipeline.TransformerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * 组装 {@link ConnectorRegistry}：注册 JDBC 源/汇（POSTGRES + MYSQL），目标写用 worker 自身 {@link DataSource}
 * （§4.1：worker 与 orchestrator 共享同一 PG 实例）。新增源类型 = 在此多注册一个实现。
 */
@Configuration
public class ConnectorConfig {

    @Bean
    public ConnectorRegistry connectorRegistry(DataSource dataSource) {
        ConnectionProvider provider = dataSource::getConnection;
        return new ConnectorRegistry()
                .register(new JdbcSourceConnector(ConnectorType.POSTGRES))
                .register(new JdbcSourceConnector(ConnectorType.MYSQL))
                .register(new JdbcSinkWriter(ConnectorType.POSTGRES, provider))
                .register(new JdbcSinkWriter(ConnectorType.MYSQL, provider));
    }

    /**
     * 转换规则链：按 {@code etl.worker.transform-profile} 挑选档位（默认 JDBC：日期归一 + 数值精度），
     * {@code keep-columns} 非空时前置字段裁剪/归一。新增模拟转换器只需在 {@link TransformerFactory} 登记。
     */
    @Bean
    public RecordTransformer recordTransformer(WorkerProperties properties) {
        return TransformerFactory.forProfile(
                DataProfile.parseOrDefault(properties.getTransformProfile()),
                properties.getKeepColumns(),
                properties.getNumberScale());
    }
}
