package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.orchestrator.outbox.OutboxPoller;
import com.lawyus.dataetl.taskmanagement.orchestrator.outbox.OutboxProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.outbox.OutboxRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2C：Outbox 入箱 → poller 真投 Kafka → 标 sent 的闭环。
 *
 * <p>直接构造 poller（IT 默认 {@code etl.outbox.enabled=false}），使用真实 Kafka/Redis/DB；
 * 验证消息 key/payload 与 worker 契约一致，且成功后不再重复投递。
 */
class OutboxDispatchIT extends AbstractOrchestratorIT {

    @Autowired
    private OutboxRepository outboxRepository;

    @Test
    @DisplayName("outbox 行被 poller 投递到 dispatch topic，成功后标 sent")
    void outboxRoundTrip() {
        String key = "it-outbox-" + UUID.randomUUID() + ":shard-0";
        String payload = "{\"taskId\":\"it-outbox-task\",\"runId\":\"it-outbox-run\","
                + "\"shardId\":\"shard-0\",\"attemptNo\":1}";
        outboxRepository.insert(ItInfra.TOPIC_DISPATCH, key, payload);

        OutboxProperties properties = new OutboxProperties();
        properties.setPollIntervalMs(60_000);
        OutboxPoller poller = new OutboxPoller(outboxRepository, properties, kafkaTemplate,
                redisson, new SimpleMeterRegistry());

        poller.poll();

        assertThat(outboxRepository.countPending()).isZero();
        assertThat(consumeByKey(key)).isEqualTo(payload);

        // 已发送行不会被再次投递
        poller.poll();
        assertThat(outboxRepository.countPending()).isZero();
    }

    private String consumeByKey(String key) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ItInfra.KAFKA_BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "outbox-it-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            consumer.subscribe(List.of(ItInfra.TOPIC_DISPATCH));
            long deadline = System.currentTimeMillis() + 15_000;
            while (System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofMillis(500))) {
                    if (key.equals(record.key())) {
                        return record.value();
                    }
                }
            }
        }
        return null;
    }
}
