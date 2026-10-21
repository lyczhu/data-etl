package com.lawyus.dataetl.taskmanagement.orchestrator.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.DispatchPublisher;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.ShardDispatchCommand;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 事务性 Outbox 派工（Phase 2C）：把派工消息写进与 {@code markAssigned} 相同的事务。
 *
 * <p>本类只做“入箱”，不做网络 IO；真正的 Kafka 投递由 {@link OutboxPoller} 异步完成。
 * 与 {@code KafkaDispatchPublisher} 二选一，由 {@code etl.outbox.enabled} 控制。
 */
@Component
@ConditionalOnProperty(prefix = "etl.outbox", name = "enabled", havingValue = "true")
public class OutboxDispatchPublisher implements DispatchPublisher {

    private final OutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;
    private final String topic;

    public OutboxDispatchPublisher(OutboxRepository outboxRepository,
                                   ObjectMapper objectMapper,
                                   @Value("${etl.topics.dispatch-batch}") String topic) {
        this.outboxRepository = outboxRepository;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    @Override
    public void publish(ShardDispatchCommand command) {
        String payload;
        try {
            payload = objectMapper.writeValueAsString(command);
        } catch (Exception e) {
            throw new IllegalStateException("serialize dispatch command failed for shard "
                    + command.getShardId(), e);
        }
        outboxRepository.insert(topic, command.getRunId() + ":" + command.getShardId(), payload);
    }
}
