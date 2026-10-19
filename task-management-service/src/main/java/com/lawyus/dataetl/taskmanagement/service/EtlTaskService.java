package com.lawyus.dataetl.taskmanagement.service;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Service
public class EtlTaskService {
    private static final Logger log = LoggerFactory.getLogger(EtlTaskService.class);

    @Autowired
    private EtlTaskRepository etlTaskRepository;

    public List<EtlTask> getAllTasks() {
        return etlTaskRepository.findAll();
    }

    public Optional<EtlTask> getTaskById(Long id) {
        return etlTaskRepository.findById(id);
    }

    public EtlTask getTaskByTaskId(String taskId) {
        return etlTaskRepository.findByTaskId(taskId);
    }

    /**
     * 生成唯一的目标表名，避免表名重复
     * 格式：源表名 + "_" + 任务id前8位
     *
     * @param sourceTableName 源表名
     * @param taskId          任务id
     * @return 唯一的目标表名
     */
    private String generateUniqueTableName(String sourceTableName, String taskId) {
        // 使用任务id的前8位作为唯一标识
        String taskIdPrefix = taskId.substring(0, 8);
        // 生成表名：源表名_任务id前8位
        return sourceTableName + "_" + taskIdPrefix;
    }

    public EtlTask createTask(EtlTask task) {
        String taskId = UUID.randomUUID().toString();
        task.setTaskId(taskId);
        task.setStatus("new");
        task.setTotalRecords(0L);
        task.setCompletedRecords(0L);

        String uniqueTableName = generateUniqueTableName(task.getSourceTableName(), taskId);
        task.setTargetTableName(uniqueTableName);
        return etlTaskRepository.save(task);
    }

    public EtlTask updateTask(Long id, EtlTask taskDetails) {
        EtlTask task = etlTaskRepository.findById(id)
                .orElseThrow(() -> new RuntimeException("Task not found with id " + id));

        task.setDataSourceId(taskDetails.getDataSourceId());
        task.setSourceTableName(taskDetails.getSourceTableName());
        task.setTargetTableName(taskDetails.getTargetTableName());
        // 封堵绕过状态机的口子（orchestrator-design.md §9）：status 与派生计数
        // （totalRecords/completedRecords）从可更新集合中移除，仅允许改非状态字段。
        // status 的唯一写路径是 OrchestratorService.transition()；计数由 checkpoint 投影。
        task.setDescription(taskDetails.getDescription());

        return etlTaskRepository.save(task);
    }

    public void deleteTask(@NonNull Long id) {
        etlTaskRepository.deleteById(id);
    }

    /**
     * 回滚任务状态为 failed（供数据拉取失败时调用）。
     * 仅当任务仍处于非终态（new/processing/running）时才回滚，避免误盖已 complete。
     */
    public void markFailed(String taskId, String reason) {
        EtlTask task = etlTaskRepository.findByTaskId(taskId);
        if (task == null) {
            log.warn("markFailed: task not found for taskId {}", taskId);
            return;
        }
        if ("completed".equals(task.getStatus()) || "failed".equals(task.getStatus())) {
            log.info("Task {} already in terminal status {}, skip rollback", taskId, task.getStatus());
            return;
        }
        task.setStatus("failed");
        task.setErrorMessage(reason);
        task.setEndTime(new Date());
        etlTaskRepository.save(task);
        log.info("Task {} rolled back to failed: {}", taskId, reason);
    }
}