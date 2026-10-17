package com.lawyus.dataetl.taskmanagement.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 消费数据拉取服务重试耗尽后发出的 task-failed 通知，将对应任务回滚为 failed。
 * 修复：原链路中 extraction 失败仅记录日志、任务卡在 running 永不回滚的问题。
 */
@Component
public class TaskFailedListener {
    private static final Logger log = LoggerFactory.getLogger(TaskFailedListener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private EtlTaskService etlTaskService;

    @KafkaListener(topics = "${etl.topics.task-failed}", groupId = "task-management-group")
    public void listen(String taskFailedMessage) {
        try {
            JsonNode root = objectMapper.readTree(taskFailedMessage);
            String taskId = root.path("taskId").asText(null);
            String reason = root.path("reason").asText("extraction failed");
            if (taskId != null && !taskId.isEmpty()) {
                etlTaskService.markFailed(taskId, reason);
            } else {
                log.warn("Received task-failed message without taskId: {}", taskFailedMessage);
            }
        } catch (Exception e) {
            log.error("Error processing task-failed message: {}", e.getMessage());
        }
    }
}
