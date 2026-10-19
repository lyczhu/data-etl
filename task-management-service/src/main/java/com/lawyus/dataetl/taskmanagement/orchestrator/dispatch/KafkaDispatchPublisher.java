package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * {@link DispatchPublisher} 的 Kafka 实现（方案 v2 WP-08 / D6 / P0-3）。
 *
 * <ul>
 *   <li>key = {@code runId:shardId}：同一 task 的不同分片分散到不同分区，修复 I1 单任务串行；</li>
 *   <li>发送<b>等待 broker ack</b>（{@code send(...).get(timeout)}）：失败抛异常，由 DispatchService
 *       保持 PENDING、不标 ASSIGNED，下轮补派；</li>
 *   <li>Phase 1B 切最小 outbox 后，本类可保持不变（publisher 接口不变）。</li>
 * </ul>
 */
@Component
public class KafkaDispatchPublisher implements DispatchPublisher {

    private static final Logger log = LoggerFactory.getLogger(KafkaDispatchPublisher.class);
    private static final long ACK_TIMEOUT_SECONDS = 5;

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
            kafkaTemplate.send(topic, key(command), payload).get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("dispatch publish failed run={} shard={}: {}",
                    command.getRunId(), command.getShardId(), e.getMessage(), e);
            throw new IllegalStateException("dispatch publish failed for shard " + command.getShardId(), e);
        }
    }

    private static String key(ShardDispatchCommand command) {
        return command.getRunId() + ":" + command.getShardId();
    }
}
