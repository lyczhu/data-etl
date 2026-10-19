package com.lawyus.dataetl.worker.report;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.worker.model.ShardProgress;
import com.lawyus.dataetl.worker.model.ShardResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * {@link WorkerEventReporter} 的 Kafka 实现（方案 §5.1：{@code etl.task.progress} / {@code etl.task.results}）。
 * key 用 runId 保证同 run 分区有序。发送失败仅记 ERROR（at-least-once 弱保证，由 sweeper/重算兜底，§6.3）。
 */
@Component
public class KafkaWorkerEventReporter implements WorkerEventReporter {

    private static final Logger log = LoggerFactory.getLogger(KafkaWorkerEventReporter.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${etl.topics.progress:etl.task.progress}")
    private String progressTopic;

    @Value("${etl.topics.results:etl.task.results}")
    private String resultsTopic;

    public KafkaWorkerEventReporter(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void reportProgress(ShardProgress progress) {
        send(progressTopic, progress.getRunId(), progress);
    }

    @Override
    public void reportResult(ShardResultMessage result) {
        send(resultsTopic, result.getRunId(), result);
    }

    private void send(String topic, String key, Object payload) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(payload));
        } catch (JsonProcessingException e) {
            log.error("serialize worker event failed for topic {}", topic, e);
        }
    }
}
