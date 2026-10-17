package com.lawyus.dataetl.dataextraction.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@EnableKafka
@Configuration
public class KafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaConfig.class);

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String groupId;

    @Value("${etl.topics.task-failed}")
    private String taskFailedTopic;

    private final KafkaTemplate<String, String> kafkaTemplate;

    public KafkaConfig(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Bean
    ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        // 设置手动确认模式
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory = 
            new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        // 设置手动确认模式（立即提交，便于失败时不提交、交由错误处理器重试）
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        // 修复：为 task-request 消费配置有界重试，超过次数后放弃并记录，避免失败消息被无限重投导致热循环
        factory.setCommonErrorHandler(kafkaErrorHandler());
        return factory;
    }

    @Bean
    CommonErrorHandler kafkaErrorHandler() {
        // 每条失败消息最多重试 3 次（间隔 1s）；耗尽后发布 task-failed 通知并跳过（不再重投）
        DefaultErrorHandler handler = new DefaultErrorHandler(this::publishTaskFailed, new FixedBackOff(1000L, 3L));
        handler.setRetryListeners((record, exception, deliveryAttempt) ->
                log.warn("Retry task-request partition={} offset={} attempt={}: {}",
                        record.topic() + "-" + record.partition(), record.offset(), deliveryAttempt, exception.getMessage()));
        return handler;
    }

    /**
     * 重试耗尽后，向 task-failed 主题发布失败通知（key 为 taskId），供 task-management 回滚任务状态。
     */
    private void publishTaskFailed(ConsumerRecord<?, ?> record, Exception exception) {
        String taskId = record.key() != null ? record.key().toString() : "unknown";
        try {
            Map<String, String> payload = new HashMap<>();
            payload.put("taskId", taskId);
            payload.put("status", "failed");
            payload.put("reason", exception == null ? "" : exception.getMessage());
            payload.put("timestamp", String.valueOf(System.currentTimeMillis()));
            kafkaTemplate.send(taskFailedTopic, taskId, new ObjectMapper().writeValueAsString(payload));
            log.error("Task {} extraction failed after retries, published to '{}' topic", taskId, taskFailedTopic);
        } catch (Exception e) {
            log.error("Failed to publish task-failed notification for task {}: {}", taskId, e.getMessage());
        }
    }
}