package com.lawyus.dataetl.worker.report;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.worker.model.ShardDltMessage;
import com.lawyus.dataetl.worker.model.ShardProgress;
import com.lawyus.dataetl.worker.model.ShardResultMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * {@link WorkerEventReporter} 的 Kafka 实现（方案 v2 WP-05/WP-08）：
 *
 * <ul>
 *   <li>key = {@code runId:shardId}，让同一 run 的不同分片分散到不同分区（修复 I1 单任务串行）；</li>
 *   <li>发送<b>等待 broker ack</b>（P0-3）：异步 send 丢失不再无感知；</li>
 *   <li>progress/results/dlt 三主题统一出口。</li>
 * </ul>
 */
@Component
public class KafkaWorkerEventReporter implements WorkerEventReporter {

    private static final Logger log = LoggerFactory.getLogger(KafkaWorkerEventReporter.class);
    private static final long ACK_TIMEOUT_SECONDS = 5;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    @Value("${etl.topics.progress:etl.task.progress}")
    private String progressTopic;

    @Value("${etl.topics.results:etl.task.results}")
    private String resultsTopic;

    @Value("${etl.topics.dlt:etl.task.dlt}")
    private String dltTopic;

    public KafkaWorkerEventReporter(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void reportProgress(ShardProgress progress) {
        send(progressTopic, key(progress.getRunId(), progress.getShardId()), progress, false);
    }

    @Override
    public void reportResult(ShardResultMessage result) {
        send(resultsTopic, key(result.getRunId(), result.getShardId()), result, true);
    }

    @Override
    public void reportDlt(ShardDltMessage dlt) {
        send(dltTopic, key(dlt.getRunId(), dlt.getShardId()), dlt, true);
    }

    private void send(String topic, String key, Object payload, boolean failLoud) {
        try {
            kafkaTemplate.send(topic, key, objectMapper.writeValueAsString(payload))
                    .get(ACK_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        } catch (JsonProcessingException e) {
            log.error("serialize worker event failed for topic {}", topic, e);
        } catch (Exception e) {
            if (failLoud) {
                log.error("send worker event failed topic={} key={}: {}", topic, key, e.getMessage(), e);
            } else {
                log.warn("send progress failed topic={} key={}: {}", topic, key, e.getMessage());
            }
        }
    }

    private static String key(String runId, String shardId) {
        return runId + ":" + shardId;
    }
}
