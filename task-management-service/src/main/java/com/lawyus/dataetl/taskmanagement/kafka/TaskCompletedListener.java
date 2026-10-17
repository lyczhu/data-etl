package com.lawyus.dataetl.taskmanagement.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Date;

@Component
public class TaskCompletedListener {
    private static final Logger log = LoggerFactory.getLogger(TaskCompletedListener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private EtlTaskRepository etlTaskRepository;
    
    /**
     * 添加新的监听器来处理任务完成消息
     */
    @KafkaListener(topics = "${etl.topics.task-completed}", groupId = "task-management-group")
    public void listenTaskCompleted(String taskCompletedMessage) {
        try {
            JsonNode rootNode = objectMapper.readTree(taskCompletedMessage);
            String taskId = rootNode.get("taskId").asText();
            long completedRecords = rootNode.get("completedRecords").asInt();
            long totalRecords = rootNode.get("totalRecords").asInt();

            if (taskId != null && !taskId.isEmpty()) {
                updateTaskStatus(taskId, completedRecords, totalRecords);
            }
        } catch (Exception e) {
            log.error("Error processing task completed message: {}", e.getMessage());
        }
    }

    public void updateTaskStatus(String taskId, long completedRecords, long totalRecords) {
        EtlTask task = etlTaskRepository.findByTaskId(taskId);
        if (task == null) {
            throw new RuntimeException("Task not found with taskId " + taskId);
        }

        // 更新数据库中的完成记录数
        task.setTotalRecords(totalRecords);
        task.setEndTime(new Date());
        task.setCompletedRecords(completedRecords);
        task.setStatus("completed");
        etlTaskRepository.save(task);
    }
}