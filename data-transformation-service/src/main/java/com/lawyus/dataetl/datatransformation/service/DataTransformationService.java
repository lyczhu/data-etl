package com.lawyus.dataetl.datatransformation.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.support.RetryTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class DataTransformationService {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    @Qualifier("kafkaSendRetryTemplate")
    private RetryTemplate kafkaSendRetryTemplate;

    @Value("${etl.topics.data-transformation}")
    private String dataTransformationTopic;

    public void processSingleRecord(String record) {
        try {
            JsonNode rootNode = objectMapper.readTree(record);
            String taskId = rootNode.get("taskId").asText();
            JsonNode data = rootNode.get("data");

            Map<String, Object> transformedData = performTransformation(data);
            Map<String, Object> newData = Map.of("taskId", taskId, "data", transformedData);

            String resultJson = objectMapper.writeValueAsString(newData);
            // 修复：原先硬编码发送到 "transformed-topic"，与下游消费的 transformed-data 不一致，导致数据链路断裂
            // 使用配置主题 + 重试模板同步发送（确保发送完成后再继续，失败最多重试 2 次）
            kafkaSendRetryTemplate.execute(ctx ->
                    kafkaTemplate.send(dataTransformationTopic, taskId, resultJson).get());
        } catch (JsonProcessingException e) {
            throw new RuntimeException("JSON 解析失败: " + record, e);
        } catch (Exception e) {
            throw new RuntimeException("数据转换或发送失败", e);
        }
    }

    private Map<String, Object> performTransformation(JsonNode rawData) throws Exception {
        Map<String, Object> transformedMap = new HashMap<>();

        rawData.fields().forEachRemaining(entry -> {
            String fieldName = entry.getKey();
            JsonNode value = entry.getValue();

            if (value.isNull()) {
                transformedMap.put(fieldName, null);
            } else if (value.isTextual()) {
                transformedMap.put(fieldName, value.asText());
            } else if (value.isNumber()) {
                // 数值字段保留两位小数
                double originalValue = value.asDouble();
                double transformedValue = Math.round(originalValue * 100.0) / 100.0;
                transformedMap.put(fieldName, transformedValue);
            } else if (value.isBoolean()) {
                transformedMap.put(fieldName, value.asBoolean());
            } else {
                // 其他类型直接转换为字符串
                transformedMap.put(fieldName, value.toString());
            }
        });
        return transformedMap;
    }
}