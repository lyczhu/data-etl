package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskDlt;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskDltRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.springframework.stereotype.Service;

import java.util.Date;
import java.util.List;

/**
 * 人工干预 API 的最小闭环（方案 v2 WP-14 / Gate 2）：{@code /progress}、{@code /cancel}、
 * {@code /failures}、{@code /retry}、{@code /resume}、{@code /abort}。
 *
 * <p>所有状态变更仍然只经 {@link OrchestratorService#transition}；本服务只做参数/前置校验与 DTO 组装。
 */
@Service
public class OrchestratorCommandService {

    private final EtlTaskRunRepository runRepository;
    private final EtlTaskCheckpointRepository checkpointRepository;
    private final EtlTaskDltRepository dltRepository;
    private final OrchestratorService orchestrator;
    private final OrchestratorProperties properties;

    public OrchestratorCommandService(EtlTaskRunRepository runRepository,
                                      EtlTaskCheckpointRepository checkpointRepository,
                                      EtlTaskDltRepository dltRepository,
                                      OrchestratorService orchestrator,
                                      OrchestratorProperties properties) {
        this.runRepository = runRepository;
        this.checkpointRepository = checkpointRepository;
        this.dltRepository = dltRepository;
        this.orchestrator = orchestrator;
        this.properties = properties;
    }

    public ProgressView progress(String taskId) {
        EtlTaskRun run = requireCurrent(taskId);
        RunAggregate a = checkpointRepository.aggregate(run.getRunId());
        return new ProgressView(taskId, run.getRunId(), run.getStatus(),
                run.getAttemptNo() == null ? 1 : run.getAttemptNo(),
                run.getShardTotal() == null ? 0 : run.getShardTotal(),
                nz(a.getPending()), nz(a.getAssigned()), nz(a.getRunning()),
                nz(a.getSucceeded()), nz(a.getFailed()), nz(a.getSkipped()),
                nz(a.getRowsIn()), nz(a.getRowsOut()), nz(a.getRowsFiltered()),
                nz(a.getRowsFailed()), nz(a.getRowsDeduped()),
                run.getCancelRequestedAt() != null, run.getLastProgressTs(), run.getErrorSummary());
    }

    public FailureView failures(String taskId) {
        EtlTaskRun run = requireCurrent(taskId);
        List<EtlTaskDlt> rows = dltRepository.findTop100ByRunIdOrderByIdAsc(run.getRunId());
        return new FailureView(taskId, run.getRunId(), run.getStatus(), run.getErrorSummary(),
                dltRepository.countUnresolved(run.getRunId()), rows);
    }

    /** 软取消：只写 {@code cancel_requested_at}，worker 在分片结束后收敛为 CANCELLED（T15/T16）。 */
    public void cancel(String taskId) {
        EtlTaskRun run = requireCurrent(taskId);
        if (run.statusEnum().isTerminal()) {
            throw new IllegalStateException("run already terminal: " + run.getStatus());
        }
        if (run.getCancelRequestedAt() != null) {
            return;   // 幂等
        }
        runRepository.markCancelRequested(run.getRunId(), new Date());
    }

    /** 人工重试：NEEDS_ATTENTION→QUEUED（T24）；PARTIAL_SUCCESS→RETRYING（T17，受 G-5 attempt 预算约束）。 */
    public void retry(String taskId) {
        EtlTaskRun run = requireCurrent(taskId);
        EtlTaskStatus from = run.statusEnum();
        if (from == EtlTaskStatus.NEEDS_ATTENTION) {
            checkpointRepository.clearLeases(run.getRunId());
            transition(run, from, EtlTaskStatus.QUEUED, "MANUAL_RETRY");
        } else if (from == EtlTaskStatus.PARTIAL_SUCCESS) {
            checkpointRepository.clearLeases(run.getRunId());
            transition(run, from, EtlTaskStatus.RETRYING, "MANUAL_RETRY");
        } else {
            throw new IllegalStateException("retry not allowed from " + from);
        }
    }

    /** 人工恢复：NEEDS_ATTENTION→RUNNING（T25，G-3 要求 quiesce）。 */
    public void resume(String taskId) {
        EtlTaskRun run = requireCurrent(taskId);
        if (run.statusEnum() != EtlTaskStatus.NEEDS_ATTENTION) {
            throw new IllegalStateException("resume only allowed from NEEDS_ATTENTION, actual=" + run.getStatus());
        }
        // WP-11：人工接管前回收旧 lease（哨兵 owner），旧 worker 的迟到写入被 lease fencing 拒绝。
        checkpointRepository.clearLeases(run.getRunId());
        transition(run, EtlTaskStatus.NEEDS_ATTENTION, EtlTaskStatus.RUNNING, "MANUAL_RESUME");
    }

    /** 人工放弃：NEEDS_ATTENTION→FAILED（T26）。 */
    public void abort(String taskId) {
        EtlTaskRun run = requireCurrent(taskId);
        if (run.statusEnum() != EtlTaskStatus.NEEDS_ATTENTION) {
            throw new IllegalStateException("abort only allowed from NEEDS_ATTENTION, actual=" + run.getStatus());
        }
        transition(run, EtlTaskStatus.NEEDS_ATTENTION, EtlTaskStatus.FAILED, "MANUAL_ABORT");
    }

    /** 带数据丢失改判成功：PARTIAL_SUCCESS→SUCCEEDED（T19，G-4/G-7 仍会拦截坏行与未解决 DLT）。 */
    public void markSucceededWithDataLoss(String taskId) {
        EtlTaskRun run = requireCurrent(taskId);
        if (run.statusEnum() != EtlTaskStatus.PARTIAL_SUCCESS) {
            throw new IllegalStateException("mark-succeeded only allowed from PARTIAL_SUCCESS, actual=" + run.getStatus());
        }
        transition(run, EtlTaskStatus.PARTIAL_SUCCESS, EtlTaskStatus.SUCCEEDED, "MANUAL_FORGIVE");
    }

    private void transition(EtlTaskRun run, EtlTaskStatus from, EtlTaskStatus to, String reason) {
        TransitionContext ctx = TransitionContext.builder(run.getTaskId())
                .reason(reason).actor("api:command").maxAttempts(properties.getMaxAttempts()).build();
        orchestrator.transition(run.getRunId(), from, to, ctx);
    }

    private EtlTaskRun requireCurrent(String taskId) {
        return runRepository.findFirstByTaskIdAndIsCurrentTrueOrderByAttemptNoDesc(taskId)
                .orElseThrow(() -> new IllegalArgumentException("no current run for task " + taskId));
    }

    private static long nz(Long v) {
        return v == null ? 0L : v;
    }

    public record ProgressView(String taskId, String runId, String status, int attemptNo, int shardTotal,
                               long pending, long assigned, long running, long succeeded, long failed,
                               long skipped, long rowsIn, long rowsOut, long rowsFiltered,
                               long rowsFailed, long rowsDeduped, boolean cancelRequested,
                               Date lastProgressTs, String errorSummary) {
    }

    public record FailureView(String taskId, String runId, String status, String errorSummary,
                              long unresolvedDlt, List<EtlTaskDlt> rows) {
    }
}
