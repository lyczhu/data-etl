package com.lawyus.dataetl.connector.spi;

import java.util.EnumMap;
import java.util.Map;

/**
 * connector 类型 → 实现的注册表（方案 §2.1 "connector-spi 避免每种源一个微服务"）。
 *
 * <p>纯内存注册，无 Spring 依赖：worker 侧用 Spring 收集所有 {@link SourceConnector}/{@link SinkWriter} bean
 * 后注册进来，或在测试里手工注册。未注册类型快速失败为 {@link UnsupportedConnectorException}。
 */
public class ConnectorRegistry {

    private final Map<ConnectorType, SourceConnector> sources = new EnumMap<>(ConnectorType.class);
    private final Map<ConnectorType, SinkWriter> sinks = new EnumMap<>(ConnectorType.class);

    public ConnectorRegistry register(SourceConnector connector) {
        if (connector != null) {
            sources.put(connector.type(), connector);
        }
        return this;
    }

    public ConnectorRegistry register(SinkWriter writer) {
        if (writer != null) {
            sinks.put(writer.type(), writer);
        }
        return this;
    }

    public SourceConnector sourceFor(ConnectorType type) {
        SourceConnector c = sources.get(type);
        if (c == null) {
            throw new UnsupportedConnectorException("No SourceConnector registered for " + type);
        }
        return c;
    }

    public SinkWriter sinkFor(ConnectorType type) {
        SinkWriter w = sinks.get(type);
        if (w == null) {
            throw new UnsupportedConnectorException("No SinkWriter registered for " + type);
        }
        return w;
    }

    public boolean supports(ConnectorType type) {
        return sources.containsKey(type) && sinks.containsKey(type);
    }
}
