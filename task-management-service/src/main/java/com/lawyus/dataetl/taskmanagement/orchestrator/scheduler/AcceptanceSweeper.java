package com.lawyus.dataetl.taskmanagement.orchestrator.scheduler;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.ErrorClass;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorMetrics;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.policy.SweeperPolicy;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.validate.RunValidator;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * 受理 sweeper（§6.5，30s）：处理 CREATED/VALIDATED 的异常驻留。
 *
 * <p><b>T30</b>：VALIDATED 驻留 &gt; validated-timeout 未入队 → 内部错误 FAILED。
 * <b>T31</b>：CREATED 驻留 &gt; created-timeout（提交时建 run 后校验链因基础设施异常未跑完）→
 * 重放 {@link RunValidator#validateAndQueue}（V-1..V-4），不消耗 attempt 预算。仅 {@code newWorker=true} 时有意义
 * （否则不存在 CREATED run）。
 */
@Component
public class AcceptanceSweeper {

    private static final Logger log = LoggerFactory.getLogger(AcceptanceSweeper.class);
    private static final String LOCK_KEY = "etl:sweeper:acceptance";

    private final RedissonClient redisson;
    private final EtlTaskRunRepository runRepository;
    private final OrchestratorService orchestrator;
    private final OrchestratorProperties properties;
    private final RunValidator runValidator;
    private final RoutingProperties routing;
    private final OrchestratorMetrics metrics;

    public AcceptanceSweeper(RedissonClient redisson,
                             EtlTaskRunRepository runRepository,
                             OrchestratorService orchestrator,
                             OrchestratorProperties properties,
                             RunValidator runValidator,
                             RoutingProperties routing,
                             OrchestratorMetrics metrics) {
        this.redisson = redisson;
        this.runRepository = runRepository;
        this.orchestrator = orchestrator;
        this.properties = properties;
        this.runValidator = runValidator;
        this.routing = routing;
        this.metrics = metrics;
    }

    @Scheduled(fixedRate = 30_000)
    public void sweep() {
        RLock lock = redisson.getLock(LOCK_KEY);
        if (!lock.tryLock()) {
            return;
        }
        try {
            Date now = new Date();
            replayStuckCreated(now);   // T31
            Date cutoff = new Date(now.getTime() - properties.getValidatedTimeout().toMillis());
            for (EtlTaskRun run : runRepository.findValidatedStuck(cutoff)) {
                if (!SweeperPolicy.isValidatedStuck(run.getUpdatedAt(), now, properties.getValidatedTimeout())) {
                    continue;
                }
                // T30：VALIDATED → FAILED，内部错误、不可重试
                TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                        .reason("ENQUEUE_STALLED").actor("sweeper:acceptance")
                        .errorClass(ErrorClass.FATAL_CONFIG).now(now)
                        .maxAttempts(properties.getMaxAttempts()).build();
                try {
                    orchestrator.transition(run.getRunId(), EtlTaskStatus.VALIDATED, EtlTaskStatus.FAILED, ctx);
                    metrics.sweeperAction("acceptance");
                    log.info("AcceptanceSweeper T30 run={} VALIDATED->FAILED (ENQUEUE_STALLED)", run.getRunId());
                } catch (GuardFailureException | IllegalStateTransitionException e) {
                    log.warn("AcceptanceSweeper T30 skipped run={} cause={}", run.getRunId(), e.getMessage());
                }
            }
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /** T31：重放 CREATED 驻留 run 的校验链（不消耗 attempt 预算）。 */
    private void replayStuckCreated(Date now) {
        if (!routing.isNewWorker()) {
            return;
        }
        Date cutoff = new Date(now.getTime() - properties.getCreatedTimeout().toMillis());
        for (EtlTaskRun run : runRepository.findCreatedStuck(cutoff)) {
            log.info("AcceptanceSweeper T31 replay validate run={} (CREATED stuck)", run.getRunId());
            try {
                runValidator.validateAndQueue(run.getTaskId(), run.getRunId());
                metrics.sweeperAction("acceptance");
            } catch (RuntimeException e) {
                log.warn("AcceptanceSweeper T31 replay failed run={}: {}", run.getRunId(), e.getMessage());
            }
        }
    }
}
