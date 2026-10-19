package com.lawyus.dataetl.taskmanagement.orchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.ConvergencePolicy;
import com.lawyus.dataetl.taskmanagement.orchestrator.ErrorClass;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorMetrics;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.RunAggregate;
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
 * 收敛监听器（{@code etl.task.results}，{@code orchestrator-design.md §7.5}）。
 *
 * <p>results 只是<b>触发器</b>：收到即对 checkpoint 做一次聚合重算（{@link ConvergencePolicy}），
 * 据实发 {@code RUNNING → SUCCEEDED/PARTIAL_SUCCESS/FAILED/CANCELLED/NEEDS_ATTENTION}（T10/T11/T14/T15/T13），
 * worker 自报的行数<b>不参与</b>终局判定（P1/D-16）。尚未收敛（仍有 PENDING/ASSIGNED/RUNNING 分片）则原样返回，
 * 等下一批 results 或 sweeper 兜底。
 *
 * <p>每条 results 代表一个分片跑完，归还 1 个令牌（{@link TokenBucket#releaseWithinCapacity} 防重复投递超发）。
 */
@Component
public class ResultsListener {

    private static final Logger log = LoggerFactory.getLogger(ResultsListener.class);

    private final ObjectMapper objectMapper;
    private final EtlTaskRunRepository runRepository;
    private final EtlTaskCheckpointRepository checkpointRepository;
    private final OrchestratorService orchestrator;
    private final OrchestratorMetrics metrics;
    private final OrchestratorProperties properties;
    private final RoutingProperties routing;
    private final TokenBucket tokenBucket;

    public ResultsListener(ObjectMapper objectMapper,
                           EtlTaskRunRepository runRepository,
                           EtlTaskCheckpointRepository checkpointRepository,
                           OrchestratorService orchestrator,
                           OrchestratorMetrics metrics,
                           OrchestratorProperties properties,
                           RoutingProperties routing,
                           TokenBucket tokenBucket) {
        this.objectMapper = objectMapper;
        this.runRepository = runRepository;
        this.checkpointRepository = checkpointRepository;
        this.orchestrator = orchestrator;
        this.metrics = metrics;
        this.properties = properties;
        this.routing = routing;
        this.tokenBucket = tokenBucket;
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
        int shardTotal = run.getShardTotal() == null ? 0 : run.getShardTotal();
        boolean cancelRequested = run.getCancelRequestedAt() != null;
        ConvergencePolicy.Outcome outcome = ConvergencePolicy.evaluate(a, shardTotal, cancelRequested);
        if (outcome == ConvergencePolicy.Outcome.NOT_CONVERGED) {
            return;
        }

        EtlTaskStatus to;
        String reason;
        ErrorClass errorClass = null;
        switch (outcome) {
            case SUCCEEDED:
                to = EtlTaskStatus.SUCCEEDED;
                reason = "ALL_SHARDS_OK";
                break;
            case PARTIAL_SUCCESS:
                to = EtlTaskStatus.PARTIAL_SUCCESS;
                reason = "SOME_SHARDS_FAILED";
                break;
            case FAILED:
                to = EtlTaskStatus.FAILED;
                reason = "ALL_SHARDS_FAILED";
                errorClass = ErrorClass.PERSISTENT;
                break;
            case CANCELLED:
                to = EtlTaskStatus.CANCELLED;
                reason = "CANCELLED_BY_USER";
                break;
            default:   // NEEDS_ATTENTION_BALANCE
                to = EtlTaskStatus.NEEDS_ATTENTION;
                reason = "ROW_BALANCE_VIOLATION";
                errorClass = ErrorClass.FATAL;
                break;
        }

        TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                .reason(reason).actor("kafka:results").errorClass(errorClass)
                .maxAttempts(properties.getMaxAttempts()).build();
        try {
            orchestrator.transition(run.getRunId(), EtlTaskStatus.RUNNING, to, ctx);
            log.info("results: run={} converged -> {} ({})", run.getRunId(), to, reason);
        } catch (GuardFailureException | IllegalStateTransitionException e) {
            // 非法迁移（P5）已由 transition 内部计数；此处不重投（ack），完整 DLT 转储随 Phase 2
            log.warn("results: convergence transition skipped run={} -> {} cause={}",
                    run.getRunId(), to, e.getMessage());
        }
    }
}
