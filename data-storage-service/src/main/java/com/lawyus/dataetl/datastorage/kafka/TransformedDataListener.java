package com.lawyus.dataetl.datastorage.kafka;

import com.lawyus.dataetl.datastorage.service.DataStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class TransformedDataListener {
    private static final Logger log = LoggerFactory.getLogger(TransformedDataListener.class);

    @Autowired
    private DataStorageService dataStorageService;

    /**
     * 批消费（Phase 0：真批入库）：整批一次交付给 storeBatch 聚合入库。
     */
    @KafkaListener(topics = "${etl.topics.data-transformation}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(List<String> transformedDataBatch) {
        try {
            dataStorageService.storeBatch(transformedDataBatch);
        } catch (Exception e) {
            // 抛出让 Kafka 重投整批；已提交行的去重标记避免重复入库
            log.error("Error processing transformed data batch ({} msgs), will retry: {}",
                    transformedDataBatch == null ? 0 : transformedDataBatch.size(), e.getMessage());
            throw new RuntimeException("Failed to store transformed data batch", e);
        }
    }
}
