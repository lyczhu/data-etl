package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskDltRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 收敛执行器（WP-10 / P1-1）：把“checkpoint 聚合 → run 终局迁移”的判定集中到一处，
 * 供 results 监听器与 {@code ConvergenceSweeper} 共用。
 *
 * <p>results 只是触发器；即使最后一条 results 丢失，sweeper 也能在无活跃分片时重算出终局。
 */
@Service
public class ConvergenceService {

    private static final Logger log = LoggerFactory.getLogger(ConvergenceService.class);

    private final OrchestratorService orchestrator;
    private final OrchestratorProperties properties;
    private final EtlTaskDltRepository dltRepository;

    public ConvergenceService(OrchestratorService orchestrator,
                              OrchestratorProperties properties,
                              EtlTaskDltRepository dltRepository) {
        this.orchestrator = orchestrator;
        this.properties = properties;
        this.dltRepository = dltRepository;
    }

    /**
     * @return true 表示已发起一次终局/重试迁移；false 表示未收敛或无需动作
     */
    public boolean converge(EtlTaskRun run, RunAggregate aggregate, String actor) {
        if (run.statusEnum() != EtlTaskStatus.RUNNING && run.statusEnum() != EtlTaskStatus.DISPATCHED) {
            return false;
        }
        if (aggregate.hasUnconverged()) {
            return false;
        }
        // 收敛但 run 仍 DISPATCHED（没有心跳）：先补 T7，再走终局迁移
        if (run.statusEnum() == EtlTaskStatus.DISPATCHED) {
            transition(run, EtlTaskStatus.DISPATCHED, EtlTaskStatus.RUNNING, "FIRST_RESULT", actor);
            run = refresh(run);
            if (run.statusEnum() != EtlTaskStatus.RUNNING) {
                return false;
            }
        }

        int shardTotal = run.getShardTotal() == null ? 0 : run.getShardTotal();
        boolean cancelRequested = run.getCancelRequestedAt() != null;
        ConvergencePolicy.Outcome outcome = ConvergencePolicy.evaluate(aggregate, shardTotal, cancelRequested);
        if (outcome == ConvergencePolicy.Outcome.NOT_CONVERGED) {
            return false;
        }
        if (outcome == ConvergencePolicy.Outcome.FAILED && canAutoRetry(run)) {
            // T12：全部分片失败且 attempt 未耗尽 → RETRYING（OrchestratorService 副作用重置 FAILED 分片）
            transition(run, EtlTaskStatus.RUNNING, EtlTaskStatus.RETRYING, "AUTO_RETRY", actor);
            return true;
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
                // WP-04：有 failed 行/未解决 DLT 时不得 SUCCEEDED；PARTIAL_SUCCESS 本身允许
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
        transition(run, EtlTaskStatus.RUNNING, to, reason, actor, errorClass);
        log.info("converge run={} -> {} ({}) actor={}", run.getRunId(), to, reason, actor);
        return true;
    }

    /** G-7：未解决 DLT > 0 时，即便行数平衡也不得 SUCCEEDED（保留数据可见性）。 */
    public boolean hasUnresolvedDlt(String runId) {
        return dltRepository.countUnresolved(runId) > 0;
    }

    private boolean canAutoRetry(EtlTaskRun run) {
        int attempt = run.getAttemptNo() == null ? 1 : run.getAttemptNo();
        return attempt < properties.getMaxAttempts();
    }

    private void transition(EtlTaskRun run, EtlTaskStatus from, EtlTaskStatus to, String reason, String actor) {
        transition(run, from, to, reason, actor, null);
    }

    private void transition(EtlTaskRun run, EtlTaskStatus from, EtlTaskStatus to, String reason,
                            String actor, ErrorClass errorClass) {
        TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                .reason(reason).actor(actor).errorClass(errorClass)
                .maxAttempts(properties.getMaxAttempts()).build();
        try {
            orchestrator.transition(run.getRunId(), from, to, ctx);
        } catch (GuardFailureException | IllegalStateTransitionException e) {
            log.warn("converge transition skipped run={} {}->{} cause={}",
                    run.getRunId(), from, to, e.getMessage());
        }
    }

    /** 迁移后重新读取 run，避免使用过期快照。 */
    private EtlTaskRun refresh(EtlTaskRun run) {
        return orchestrator.currentRun(run.getTaskId()).orElse(run);
    }
}
