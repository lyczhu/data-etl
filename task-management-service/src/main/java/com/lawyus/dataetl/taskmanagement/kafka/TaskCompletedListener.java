package com.lawyus.dataetl.taskmanagement.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.adapter.LegacyStatusAdapter;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Optional;

/**
 * 旧链路"任务完成"监听器（{@code task-completed}）。
 *
 * <p>影子切换（{@code orchestrator-design.md §11.2/D-25}）：
 * <ul>
 *   <li>{@code etl.routing.newWorker=false}（默认）：保持现网行为——直接写 {@code etl_task} 的 status/计数。</li>
 *   <li>{@code true}：不直写 status，改经 {@link LegacyStatusAdapter} → {@link OrchestratorService#transition}
 *       收敛到状态机唯一入口（P2）。run 缺失或已终态则跳过（results 路径通常已先行收敛）。</li>
 * </ul>
 */
@Component
public class TaskCompletedListener {
    private static final Logger log = LoggerFactory.getLogger(TaskCompletedListener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private EtlTaskRepository etlTaskRepository;

    @Autowired
    private OrchestratorService orchestrator;

    @Autowired
    private RoutingProperties routing;

    @KafkaListener(topics = "${etl.topics.task-completed}", groupId = "task-management-group")
    public void listenTaskCompleted(String taskCompletedMessage) {
        try {
            JsonNode rootNode = objectMapper.readTree(taskCompletedMessage);
            String taskId = rootNode.path("taskId").asText(null);
            long completedRecords = rootNode.path("completedRecords").asLong();
            long totalRecords = rootNode.path("totalRecords").asLong();

            if (taskId != null && !taskId.isEmpty()) {
                if (routing.isNewWorker()) {
                    routeToStateMachine(taskId);
                } else {
                    updateTaskStatus(taskId, completedRecords, totalRecords);
                }
            }
        } catch (Exception e) {
            log.error("Error processing task completed message: {}", e.getMessage());
        }
    }

    /** 旧链路 COMPLETED → 状态机（经唯一入口 transition，不直写 status）。 */
    private void routeToStateMachine(String taskId) {
        Optional<EtlTaskRun> runOpt = orchestrator.currentRun(taskId);
        if (runOpt.isEmpty()) {
            log.warn("task-completed(new): no run for task={}, skip state-machine routing", taskId);
            return;
        }
        EtlTaskRun run = runOpt.get();
        EtlTaskStatus current = run.statusEnum();
        if (current.isTerminal()) {
            return;   // 已终态，幂等忽略
        }
        LegacyStatusAdapter.Intent intent =
                LegacyStatusAdapter.toTransition(LegacyStatusAdapter.LegacySignal.COMPLETED);
        TransitionContext ctx = TransitionContext.builder(taskId)
                .reason(intent.reason()).actor("kafka:legacy-completed").errorClass(intent.errorClass()).build();
        try {
            orchestrator.transition(run.getRunId(), current, intent.to(), ctx);
        } catch (IllegalStateTransitionException | GuardFailureException e) {
            // 旧链路无 checkpoint → G-4 可能不满足；此处不改状态，交由 results/sweeper 收敛（§11.1 注）
            log.warn("task-completed(new): transition skipped run={} {}->{} cause={}",
                    run.getRunId(), current, intent.to(), e.getMessage());
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
