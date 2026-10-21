package com.lawyus.dataetl.taskmanagement.controller;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.audit.AuditService;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorCommandService;
import com.lawyus.dataetl.taskmanagement.orchestrator.target.TargetTableConflictException;
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/tasks")
public class EtlTaskController {
    @Autowired
    private EtlTaskService etlTaskService;

    @Autowired
    private OrchestratorCommandService commandService;

    @Autowired
    private AuditService auditService;

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

    /** Phase 2E：目标表归属冲突 → 409；冻结字段被修改 → 400。 */
    @ExceptionHandler(TargetTableConflictException.class)
    public ResponseEntity<String> targetTableConflict(TargetTableConflictException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(e.getMessage());
    }

    // --- 编排器人工干预 API（WP-14 / Gate 2 最小闭环） ---

    @GetMapping("/{taskId}/progress")
    public ResponseEntity<?> progress(@PathVariable String taskId) {
        return execute(() -> ResponseEntity.ok(commandService.progress(taskId)));
    }

    @GetMapping("/{taskId}/failures")
    public ResponseEntity<?> failures(@PathVariable String taskId) {
        return execute(() -> ResponseEntity.ok(commandService.failures(taskId)));
    }

    /** Phase 2B：状态迁移审计（INV-9），按 id 倒序分页。 */
    @GetMapping("/{taskId}/audit")
    public ResponseEntity<?> audit(@PathVariable String taskId,
                                   @RequestParam(defaultValue = "0") int page,
                                   @RequestParam(defaultValue = "50") int size) {
        return execute(() -> ResponseEntity.ok(auditService.page(taskId, page, size)));
    }

    @PostMapping("/{taskId}/cancel")
    public ResponseEntity<?> cancel(@PathVariable String taskId) {
        return execute(() -> {
            commandService.cancel(taskId);
            return ResponseEntity.accepted().body("CANCEL_REQUESTED");
        });
    }

    @PostMapping("/{taskId}/retry")
    public ResponseEntity<?> retry(@PathVariable String taskId) {
        return execute(() -> {
            commandService.retry(taskId);
            return ResponseEntity.accepted().body("RETRY_ACCEPTED");
        });
    }

    @PostMapping("/{taskId}/resume")
    public ResponseEntity<?> resume(@PathVariable String taskId) {
        return execute(() -> {
            commandService.resume(taskId);
            return ResponseEntity.accepted().body("RESUMED");
        });
    }

    @PostMapping("/{taskId}/abort")
    public ResponseEntity<?> abort(@PathVariable String taskId) {
        return execute(() -> {
            commandService.abort(taskId);
            return ResponseEntity.accepted().body("ABORTED");
        });
    }

    @PostMapping("/{taskId}/mark-succeeded-with-data-loss")
    public ResponseEntity<?> markSucceeded(@PathVariable String taskId) {
        return execute(() -> {
            commandService.markSucceededWithDataLoss(taskId);
            return ResponseEntity.accepted().body("MARKED_SUCCEEDED");
        });
    }

    /** 统一错误映射：无 current run → 404；非法状态/guard → 409。 */
    private ResponseEntity<?> execute(CommandCall call) {
        try {
            return call.run();
        } catch (IllegalArgumentException e) {
            return ResponseEntity.status(404).body(e.getMessage());
        } catch (IllegalStateException | GuardFailureException | IllegalStateTransitionException e) {
            return ResponseEntity.status(409).body(e.getMessage());
        }
    }

    @FunctionalInterface
    private interface CommandCall {
        ResponseEntity<?> run();
    }
}
