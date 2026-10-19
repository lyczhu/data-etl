package com.lawyus.dataetl.taskmanagement.orchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.ConvergenceService;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.RunAggregate;
import com.lawyus.dataetl.taskmanagement.orchestrator.RunCounterProjector;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.TokenBucket;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.ResultMessage;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * 收敛监听器（{@code etl.task.results}，{@code orchestrator-design.md §7.5} / 方案 v2 WP-05/WP-10）。
 *
 * <p>results 只是<b>触发器</b>：收到即对 checkpoint 做一次聚合重算，交给 {@link ConvergenceService}
 * 决定终局（或 T12 自动重试）。worker 自报的行数<b>不参与</b>终局判定（P1/D-16）。
 *
 * <p>每条 results 代表一个分片跑完，归还 1 个令牌（{@link TokenBucket#releaseWithinCapacity} 防重复投递超发）。
 * 聚合结果同时用于 {@link RunCounterProjector} 的计数投影（含未收敛的中间态）。
 *
 * <p>最后一条 results 丢失由 {@code ConvergenceSweeper} 兜底（同一 {@link ConvergenceService}）。
 */
@Component
public class ResultsListener {

    private static final Logger log = LoggerFactory.getLogger(ResultsListener.class);

    private final ObjectMapper objectMapper;
    private final EtlTaskRunRepository runRepository;
    private final EtlTaskCheckpointRepository checkpointRepository;
    private final OrchestratorService orchestrator;
    private final OrchestratorProperties properties;
    private final RoutingProperties routing;
    private final TokenBucket tokenBucket;
    private final RunCounterProjector counterProjector;
    private final ConvergenceService convergenceService;

    public ResultsListener(ObjectMapper objectMapper,
                           EtlTaskRunRepository runRepository,
                           EtlTaskCheckpointRepository checkpointRepository,
                           OrchestratorService orchestrator,
                           OrchestratorProperties properties,
                           RoutingProperties routing,
                           TokenBucket tokenBucket,
                           RunCounterProjector counterProjector,
                           ConvergenceService convergenceService) {
        this.objectMapper = objectMapper;
        this.runRepository = runRepository;
        this.checkpointRepository = checkpointRepository;
        this.orchestrator = orchestrator;
        this.properties = properties;
        this.routing = routing;
        this.tokenBucket = tokenBucket;
        this.counterProjector = counterProjector;
        this.convergenceService = convergenceService;
    }

    @KafkaListener(topics = "${etl.topics.results}", groupId = "task-management-group")
    public void listen(String json) {
        if (!routing.isNewWorker()) {
            return;
        }
        ResultMessage msg;
        try {
            msg = objectMapper.readValue(json, ResultMessage.class);
        } catch (Exception parseFailure) {
            log.warn("results: unparsable payload skipped: {}", parseFailure.getMessage());
            return;
        }
        if (msg.getRunId() == null || msg.getRunId().isEmpty()) {
            log.warn("results: message without runId skipped");
            return;
        }
        handle(msg);
    }

    void handle(ResultMessage msg) {
        // 一个分片跑完 → 归还一个令牌（钳制到容量，容忍重复 results）
        tokenBucket.releaseWithinCapacity(1);

        Optional<EtlTaskRun> found = runRepository.findByRunId(msg.getRunId());
        if (found.isEmpty()) {
            log.warn("results: run not found runId={}", msg.getRunId());
            return;
        }
        EtlTaskRun run = found.get();

        // results 隐含 worker 已在跑：若仍在 DISPATCHED 先补 T7（否则永不收敛）
        if (run.statusEnum() == EtlTaskStatus.DISPATCHED) {
            TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                    .reason("FIRST_RESULT").actor("kafka:results").maxAttempts(properties.getMaxAttempts()).build();
            try {
                orchestrator.transition(run.getRunId(), EtlTaskStatus.DISPATCHED, EtlTaskStatus.RUNNING, ctx);
                run = runRepository.findByRunId(msg.getRunId()).orElse(run);
            } catch (GuardFailureException | IllegalStateTransitionException e) {
                log.warn("results: T7 backfill skipped run={} cause={}", run.getRunId(), e.getMessage());
            }
        }

        if (run.statusEnum() != EtlTaskStatus.RUNNING) {
            return;   // 终态/其它态：幂等忽略
        }

        RunAggregate a = checkpointRepository.aggregate(run.getRunId());
        // 计数投影放在收敛门之前：未定局的中间态也要随分片推进更新 run 行计数（复用现成聚合值，不重查）。
        counterProjector.project(run.getRunId(), a);
        convergenceService.converge(run, a, "kafka:results");
    }
}
