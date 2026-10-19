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
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * 消费数据拉取服务重试耗尽后发出的 task-failed 通知。
 * 修复：原链路中 extraction 失败仅记录日志、任务卡在 running 永不回滚的问题。
 *
 * <p>影子切换（{@code orchestrator-design.md §11.2/D-25}）：{@code newWorker=false} 走旧 {@code markFailed} 直写；
 * {@code true} 经 {@link LegacyStatusAdapter}（FAILED → {@code NEEDS_ATTENTION}，"宁可多告不漏告" §11.1）收敛到
 * {@link OrchestratorService#transition} 唯一入口，不直写 status。
 */
@Component
public class TaskFailedListener {
    private static final Logger log = LoggerFactory.getLogger(TaskFailedListener.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private EtlTaskService etlTaskService;

    @Autowired
    private OrchestratorService orchestrator;

    @Autowired
    private RoutingProperties routing;

    @KafkaListener(topics = "${etl.topics.task-failed}", groupId = "task-management-group")
    public void listen(String taskFailedMessage) {
        try {
            JsonNode root = objectMapper.readTree(taskFailedMessage);
            String taskId = root.path("taskId").asText(null);
            String reason = root.path("reason").asText("extraction failed");
            if (taskId == null || taskId.isEmpty()) {
                log.warn("Received task-failed message without taskId: {}", taskFailedMessage);
                return;
            }
            if (routing.isNewWorker()) {
                routeToStateMachine(taskId, reason);
            } else {
                etlTaskService.markFailed(taskId, reason);
            }
        } catch (Exception e) {
            log.error("Error processing task-failed message: {}", e.getMessage());
        }
    }

    private void routeToStateMachine(String taskId, String reason) {
        Optional<EtlTaskRun> runOpt = orchestrator.currentRun(taskId);
        if (runOpt.isEmpty()) {
            log.warn("task-failed(new): no run for task={}, skip state-machine routing", taskId);
            return;
        }
        EtlTaskRun run = runOpt.get();
        EtlTaskStatus current = run.statusEnum();
        if (current.isTerminal()) {
            return;
        }
        LegacyStatusAdapter.Intent intent =
                LegacyStatusAdapter.toTransition(LegacyStatusAdapter.LegacySignal.FAILED);
        TransitionContext ctx = TransitionContext.builder(taskId)
                .reason(intent.reason() + ":" + reason).actor("kafka:legacy-failed")
                .errorClass(intent.errorClass()).build();
        try {
            orchestrator.transition(run.getRunId(), current, intent.to(), ctx);
        } catch (IllegalStateTransitionException | GuardFailureException e) {
            log.warn("task-failed(new): transition skipped run={} {}->{} cause={}",
                    run.getRunId(), current, intent.to(), e.getMessage());
        }
    }
}
