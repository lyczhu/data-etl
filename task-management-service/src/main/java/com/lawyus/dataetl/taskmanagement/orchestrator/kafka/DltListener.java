package com.lawyus.dataetl.taskmanagement.orchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.ErrorClass;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorMetrics;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.TokenBucket;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.DltMessage;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * DLT 监听器（{@code etl.task.dlt}，{@code orchestrator-design.md §7.6}）。严格区分两类 DLT，避免误报与死循环：
 *
 * <ul>
 *   <li><b>行级毒 / 消费层毒</b>（{@code FATAL}）：触发 {@code → NEEDS_ATTENTION}（T13，"DLT 非空即需人工"）；
 *       完整 {@code etl_task_dlt} 落表与 {@code resolved_at}/{@code abandoned} 回写随 Phase 2，Phase 1 先做控制动作。</li>
 *   <li><b>非法迁移 / 协议违例</b>（{@code ILLEGAL_TRANSITION}/{@code PROTOCOL_VIOLATION}）：仅告警 + 指标，
 *       <b>不动业务状态</b>（§7.6 表：不粉饰为任务失败）。</li>
 * </ul>
 *
 * <p>幂等：终态 run 直接忽略；{@code CREATED/VALIDATED} 无到 NEEDS_ATTENTION 的边，{@link IllegalStateTransitionException}
 * 被捕获后跳过（不重投，ack）。毒分片跑完 → 归还一个令牌。
 */
@Component
public class DltListener {

    private static final Logger log = LoggerFactory.getLogger(DltListener.class);

    private final ObjectMapper objectMapper;
    private final EtlTaskRunRepository runRepository;
    private final OrchestratorService orchestrator;
    private final OrchestratorMetrics metrics;
    private final OrchestratorProperties properties;
    private final RoutingProperties routing;
    private final TokenBucket tokenBucket;

    public DltListener(ObjectMapper objectMapper,
                       EtlTaskRunRepository runRepository,
                       OrchestratorService orchestrator,
                       OrchestratorMetrics metrics,
                       OrchestratorProperties properties,
                       RoutingProperties routing,
                       TokenBucket tokenBucket) {
        this.objectMapper = objectMapper;
        this.runRepository = runRepository;
        this.orchestrator = orchestrator;
        this.metrics = metrics;
        this.properties = properties;
        this.routing = routing;
        this.tokenBucket = tokenBucket;
    }

    @KafkaListener(topics = "${etl.topics.dlt}", groupId = "task-management-group")
    public void listen(String json) {
        if (!routing.isNewWorker()) {
            return;
        }
        DltMessage msg;
        try {
            msg = objectMapper.readValue(json, DltMessage.class);
        } catch (Exception parseFailure) {
            log.warn("dlt: unparsable payload skipped: {}", parseFailure.getMessage());
            return;
        }
        if (msg.getRunId() == null || msg.getRunId().isEmpty()) {
            log.warn("dlt: message without runId, nothing to attach");
            return;
        }
        try {
            KafkaMdc.put(msg.getTaskId(), msg.getRunId(), msg.getShardId(), null);
            handle(msg);
        } finally {
            KafkaMdc.clear();
        }
    }

    void handle(DltMessage msg) {
        metrics.dltReceived();
        // 协议类 DLT（非法迁移/违例）：仅告警，不动状态（§7.6）
        if (isProtocolOnly(msg.getErrorClass())) {
            log.warn("dlt: protocol violation recorded (no state change) run={} shard={} class={}",
                    msg.getRunId(), msg.getShardId(), msg.getErrorClass());
            return;
        }
        tokenBucket.releaseWithinCapacity(1);   // 毒分片终止，归还令牌

        Optional<EtlTaskRun> found = runRepository.findByRunId(msg.getRunId());
        if (found.isEmpty()) {
            log.warn("dlt: run not found runId={}", msg.getRunId());
            return;
        }
        EtlTaskRun run = found.get();
        KafkaMdc.putAttempt(run.getAttemptNo());
        EtlTaskStatus current = run.statusEnum();
        if (current.isTerminal()) {
            return;   // 终态不可回退
        }
        TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                .reason("DLT_ROW").actor("kafka:dlt").errorClass(ErrorClass.FATAL)
                .maxAttempts(properties.getMaxAttempts()).build();
        try {
            orchestrator.transition(run.getRunId(), current, EtlTaskStatus.NEEDS_ATTENTION, ctx);
            log.info("dlt: run={} {} -> NEEDS_ATTENTION (DLT non-empty)", run.getRunId(), current);
        } catch (GuardFailureException | IllegalStateTransitionException e) {
            log.warn("dlt: NEEDS_ATTENTION skipped run={} from={} cause={}", run.getRunId(), current, e.getMessage());
        }
    }

    private static boolean isProtocolOnly(String errorClass) {
        return ErrorClass.ILLEGAL_TRANSITION.name().equalsIgnoreCase(errorClass)
                || "PROTOCOL_VIOLATION".equalsIgnoreCase(errorClass);
    }
}
