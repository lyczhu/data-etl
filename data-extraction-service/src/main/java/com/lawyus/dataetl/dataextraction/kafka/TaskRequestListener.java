package com.lawyus.dataetl.dataextraction.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.dataextraction.entity.EtlTaskMessage;
import com.lawyus.dataetl.dataextraction.service.DataExtractionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class TaskRequestListener {
    private static final Logger log = LoggerFactory.getLogger(TaskRequestListener.class);
    
    @Autowired
    private DataExtractionService dataExtractionService;

    @KafkaListener(topics = "${etl.topics.task-request}", groupId = "${spring.kafka.consumer.group-id}", containerFactory = "kafkaListenerContainerFactory")
    public void listen(String taskRequest, Acknowledgment ack) {
        ObjectMapper om = new ObjectMapper();
        EtlTaskMessage taskMessage;
        try {
            taskMessage = om.readValue(taskRequest, EtlTaskMessage.class);
        } catch (JsonProcessingException e) {
            // 不可重试（消息体本身非法）：直接确认并丢弃，避免毒消息阻塞分区
            log.error("Error parsing TaskRequest message, discarding: {}", e.getMessage());
            ack.acknowledge();
            return;
        }
        try {
            dataExtractionService.processTask(taskMessage);
            ack.acknowledge();
        } catch (Exception e) {
            // 修复：不再吞异常也不确认——上抛交由 DefaultErrorHandler 有界重试，耗尽后记录并跳过，
            // 消除原“既不重试也无回滚、且位点不提交导致无限重投”的问题
            log.error("Error processing task request, will retry: {}", e.getMessage());
            throw new RuntimeException("Failed to process task request", e);
        }
    }
}