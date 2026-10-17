package com.lawyus.dataetl.datatransformation.kafka;

import com.lawyus.dataetl.datatransformation.service.DataTransformationService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.stream.Collectors;

@Component
public class RawDataListener {
    private static final Logger log = LoggerFactory.getLogger(RawDataListener.class);

    @Autowired
    private DataTransformationService transformService;

    @Autowired
    @Qualifier("etlTransformExecutor")
    private Executor transformExecutor;

    @KafkaListener(topics = "${etl.topics.data-extraction}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaManualAckListenerContainerFactory")
    public void listen(List<String> payloads, Acknowledgment ack) {
        List<CompletableFuture<Void>> futures = payloads.stream()
                .map(payload -> CompletableFuture.runAsync(() ->
                                transformService.processSingleRecord(payload), transformExecutor)
                        .exceptionally(ex -> {
                            log.error("处理记录失败: {}", payload, ex);
                            return null;
                        }))
                .toList();

        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        ack.acknowledge();
        log.info("批次处理完成，已提交偏移量，记录数: {}", payloads.size());
    }
}