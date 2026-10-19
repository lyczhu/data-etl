package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.springframework.stereotype.Component;

/**
 * run 行计数列的投影器：把 {@code etl_task_checkpoint} 的聚合值写到 {@code etl_task_run} 的汇总列
 * （{@code shard_succeeded/failed/skipped}、{@code rows_in/out/dropped/deduped}，
 * {@code orchestrator-design.md §4.2}）。
 *
 * <p>没有这层投影，run 行的这些列会永远停在 {@code newRun} 写入的 0，于是
 * §12.2 校验 2（{@code ru.rows_out + ru.rows_dropped <> ru.rows_in}）因两边同为 0 而
 * <b>恒空转通过</b>——§12.1 全链路 E2E 首轮核实发现正是这个形态。
 *
 * <p>两个调用点，语义不同且都需要：
 * <ol>
 *   <li>{@code ResultsListener} —— 每收到一条 results 就投影一次（含<b>尚未收敛</b>的中间态），
 *       使 run 行的进度计数随分片推进，而不只在终态才准；</li>
 *   <li>{@code OrchestratorService#applySideEffects} —— 进入 {@code SUCCEEDED/
 *       PARTIAL_SUCCESS/FAILED/CANCELLED/NEEDS_ATTENTION} 时再投影一次作<b>终态快照</b>，
 *       覆盖"sweeper 驱动的终局根本没有 results 消息"的路径（T9/T13/T16）。</li>
 * </ol>
 *
 * <p>投影是<b>绝对值覆写</b>（见 {@code EtlTaskRunRepository#projectCounters}），因此至少一次投递下
 * 重复投影无害；它也不写 status，故不构成第二状态写路径（§11.3）。
 */
@Component
public class RunCounterProjector {

    private final EtlTaskRunRepository runRepository;
    private final EtlTaskCheckpointRepository checkpointRepository;

    public RunCounterProjector(EtlTaskRunRepository runRepository,
                               EtlTaskCheckpointRepository checkpointRepository) {
        this.runRepository = runRepository;
        this.checkpointRepository = checkpointRepository;
    }

    /** 用调用方<b>现成</b>的聚合值投影，避免同一条消息上重复聚合（results 路径每条消息都要投影）。 */
    public void project(String runId, RunAggregate aggregate) {
        runRepository.projectCounters(runId,
                asInt(aggregate.getSucceeded()),
                asInt(aggregate.getFailed()),
                asInt(aggregate.getSkipped()),
                asLong(aggregate.getRowsIn()),
                asLong(aggregate.getRowsOut()),
                asLong(aggregate.getRowsDropped()),
                asLong(aggregate.getRowsDeduped()));
    }

    /** 重新聚合后投影：调用方手上没有聚合值时使用（终态快照）。 */
    public void project(String runId) {
        project(runId, checkpointRepository.aggregate(runId));
    }

    /**
     * 分片计数落库为 {@code INT}：聚合值来自同一 run 的分片行数，上界即 {@code shard_total}，
     * 收窄到 int 无溢出风险（分片数不可能超过 2^31）。
     */
    private static int asInt(Long value) {
        return value == null ? 0 : Math.toIntExact(value);
    }

    private static long asLong(Long value) {
        return value == null ? 0L : value;
    }
}
