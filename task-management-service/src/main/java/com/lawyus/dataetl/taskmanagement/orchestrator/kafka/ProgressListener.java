package com.lawyus.dataetl.taskmanagement.orchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorMetrics;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.ProgressMessage;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.Optional;

/**
 * 心跳/进度监听器（{@code etl.task.progress}，{@code orchestrator-design.md §7.3}）。
 *
 * <p>职责<b>仅两件</b>：① 首个心跳把 {@code DISPATCHED → RUNNING}（T7）；② 单调刷新 {@code last_progress_ts}
 * 与 checkpoint {@code updated_at}（卡死判定基线）。progress <b>不推进其它任何状态</b>——分片终态由 worker 直写
 * checkpoint，进度丢失只影响展示时效、不影响正确性（P1）。
 *
 * <p>Poison Guard：非法 JSON 记 WARN 后跳过（不抛错致 poison loop，§6.4/D-13）。RETRYING 窗口内收到活跃
 * 心跳视为协议违例（INV-5），计指标 + 告警（完整 DLT 转储随 Phase 2）。
 */
@Component
public class ProgressListener {

    private static final Logger log = LoggerFactory.getLogger(ProgressListener.class);

    private final ObjectMapper objectMapper;
    private final EtlTaskRunRepository runRepository;
    private final EtlTaskCheckpointRepository checkpointRepository;
    private final OrchestratorService orchestrator;
    private final OrchestratorMetrics metrics;
    private final OrchestratorProperties properties;
    private final RoutingProperties routing;

    public ProgressListener(ObjectMapper objectMapper,
                            EtlTaskRunRepository runRepository,
                            EtlTaskCheckpointRepository checkpointRepository,
                            OrchestratorService orchestrator,
                            OrchestratorMetrics metrics,
                            OrchestratorProperties properties,
                            RoutingProperties routing) {
        this.objectMapper = objectMapper;
        this.runRepository = runRepository;
        this.checkpointRepository = checkpointRepository;
        this.orchestrator = orchestrator;
        this.metrics = metrics;
        this.properties = properties;
        this.routing = routing;
    }

    @KafkaListener(topics = "${etl.topics.progress}", groupId = "task-management-group")
    public void listen(String json) {
        if (!routing.isNewWorker()) {
            return;
        }
        ProgressMessage msg;
        try {
            msg = objectMapper.readValue(json, ProgressMessage.class);
        } catch (Exception parseFailure) {
            log.warn("progress: unparsable payload skipped: {}", parseFailure.getMessage());
            return;
        }
        if (msg.getRunId() == null || msg.getRunId().isEmpty()) {
            log.warn("progress: message without runId skipped");
            return;
        }
        handle(msg);
    }

    void handle(ProgressMessage msg) {
        Optional<EtlTaskRun> found = runRepository.findByRunId(msg.getRunId());
        if (found.isEmpty()) {
            log.warn("progress: run not found runId={}", msg.getRunId());
            return;
        }
        EtlTaskRun run = found.get();
        EtlTaskStatus status = run.statusEnum();
        Date ts = msg.getTimestamp() > 0 ? new Date(msg.getTimestamp()) : new Date();

        if (status == EtlTaskStatus.DISPATCHED) {
            // T7 首次心跳确认
            TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                    .reason("FIRST_HEARTBEAT").actor("kafka:progress").now(ts)
                    .maxAttempts(properties.getMaxAttempts()).build();
            try {
                orchestrator.transition(run.getRunId(), EtlTaskStatus.DISPATCHED, EtlTaskStatus.RUNNING, ctx);
            } catch (GuardFailureException | IllegalStateTransitionException e) {
                log.warn("progress: T7 skipped run={} cause={}", run.getRunId(), e.getMessage());
            }
        } else if (status == EtlTaskStatus.RETRYING) {
            // INV-5：退避窗口内不应有活跃分片
            metrics.protocolViolation();
            log.warn("progress: protocol violation (active shard in RETRYING) run={} shard={}",
                    run.getRunId(), msg.getShardId());
        }

        // 心跳基线单调更新 + checkpoint 刷新（不改 shard 状态）
        runRepository.touchProgress(run.getRunId(), ts);
        if (msg.getShardId() != null) {
            checkpointRepository.touch(run.getRunId(), msg.getShardId(), ts);
        }
    }
}
