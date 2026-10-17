package com.lawyus.dataetl.taskmanagement.controller;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/tasks")
public class EtlTaskController {
    @Autowired
    private EtlTaskService etlTaskService;

    @GetMapping
    public List<EtlTask> getAllTasks() {
        return etlTaskService.getAllTasks();
    }

    @GetMapping("/{id}")
    public ResponseEntity<EtlTask> getTaskById(@PathVariable(value = "id") Long taskId) {
        EtlTask task = etlTaskService.getTaskById(taskId)
                .orElseThrow(() -> new RuntimeException("Task not found with id " + taskId));
        return ResponseEntity.ok().body(task);
    }

    @GetMapping("/task-id/{taskId}")
    public ResponseEntity<EtlTask> getTaskByTaskId(@PathVariable(value = "taskId") String taskId) {
        EtlTask task = etlTaskService.getTaskByTaskId(taskId);
        if (task == null) {
            return ResponseEntity.notFound().build();
        }
        return ResponseEntity.ok().body(task);
    }

    @PostMapping
    public EtlTask createTask(@RequestBody EtlTask task) {
        return etlTaskService.createTask(task);
    }

    @PutMapping("/{id}")
    public ResponseEntity<EtlTask> updateTask(@PathVariable(value = "id") Long taskId,
                                                   @RequestBody EtlTask taskDetails) {
        EtlTask updatedTask = etlTaskService.updateTask(taskId, taskDetails);
        return ResponseEntity.ok(updatedTask);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteTask(@PathVariable(value = "id") Long taskId) {
        etlTaskService.deleteTask(taskId);
        return ResponseEntity.noContent().build();
    }
}
