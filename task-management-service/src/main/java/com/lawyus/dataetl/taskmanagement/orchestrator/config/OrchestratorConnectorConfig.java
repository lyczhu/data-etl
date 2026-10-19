package com.lawyus.dataetl.taskmanagement.orchestrator.config;

import com.lawyus.dataetl.connector.spi.ConnectorRegistry;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.jdbc.JdbcSourceConnector;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 编排器侧的 connector 注册表（{@code orchestrator-design.md §7.1} V-2/V-4 用）：只登记 <b>源</b> connector，
 * 供 validate 阶段 {@code readSchema}/{@code planShards} 生成 JDBC 分片计划。
 *
 * <p>编排器只做计划与派工，<b>不写数据/不建目标表</b>（写权矩阵 §4.1），故不注册 {@code SinkWriter}；
 * 对 sink 的查找会快速失败（{@code UnsupportedConnectorException}），这是预期边界。Phase 2 接入 Mongo/File 时在此多注册。
 */
@Configuration
public class OrchestratorConnectorConfig {

    @Bean
    public ConnectorRegistry orchestratorConnectorRegistry() {
        return new ConnectorRegistry()
                .register(new JdbcSourceConnector(ConnectorType.POSTGRES))
                .register(new JdbcSourceConnector(ConnectorType.MYSQL));
    }
}
