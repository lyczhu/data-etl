package com.lawyus.dataetl.datastorage.kafka;

import com.lawyus.dataetl.datastorage.service.DataStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class TransformedDataListener {
    private static final Logger log = LoggerFactory.getLogger(TransformedDataListener.class);

    @Autowired
    private DataStorageService dataStorageService;

    @KafkaListener(topics = "${etl.topics.data-transformation}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(String transformedData) {
        try {
            dataStorageService.storeData(transformedData);
        } catch (Exception e) {
            // 修复：原实现吞掉异常会导致位点提交、消息丢失；抛出后由 Kafka 错误处理器重试
            // （DataStorageService 已基于 taskId+id 做幂等去重，重试不会重复入库）
            log.error("Error processing transformed data, will retry: {}", e.getMessage());
            throw new RuntimeException("Failed to store transformed data", e);
        }
    }
}
