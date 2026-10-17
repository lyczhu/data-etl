package com.lawyus.dataetl.datastorage.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "task-management-service")
public interface TaskManagementServiceClient {
    
    @GetMapping("/api/tasks/task-id/{taskId}")
    EtlTask getTaskByTaskId(@PathVariable("taskId") String taskId);
}