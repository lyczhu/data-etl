package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * {@link DispatchPublisher} 的 Kafka 实现（Phase 1 直投，{@code orchestrator-design.md §6.3}）。
 * 发送失败仅记 ERROR（弱 at-least-once），由 sweeper（T8 心跳超时回退）与收敛重算兜底——派工丢失
 * 表现为该 shard 停在 ASSIGNED/PENDING，最终被 DispatchTimeoutSweeper 拉回重派，不产生错误终态。
 */
@Component
public class KafkaDispatchPublisher implements DispatchPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaDispatchPublisher.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;
    private final String topic;

    public KafkaDispatchPublisher(KafkaTemplate<String, String> kafkaTemplate,
                                  ObjectMapper objectMapper,
                                  @Value("${etl.topics.dispatch-batch}") String topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
        this.topic = topic;
    }

    @Override
    public void publish(ShardDispatchCommand command) {
        try {
            String payload = objectMapper.writeValueAsString(command);
            kafkaTemplate.send(topic, command.getTaskId(), payload);
        } catch (Exception e) {
            log.error("dispatch publish failed run={} shard={}: {}",
                    command.getRunId(), command.getShardId(), e.getMessage(), e);
        }
    }
}
