package com.lawyus.dataetl.taskmanagement.orchestrator.repository;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * {@link EtlTaskRun} 仓储。状态的<b>唯一</b>写入口是 {@link #casStatus}（乐观并发 CAS），
 * 对应 {@code orchestrator-design.md §6.1} 的 P4：CAS 返回 0 表示并发已被覆盖，调用方按幂等丢弃处理。
 *
 * <p>其余 {@code @Modifying} 方法只做同一迁移事务内的副作用（时间戳、attempt、reason），
 * 一律不改 {@code status}，以维持"transition 是唯一状态写路径"的守卫（§11.3）。
 */
@Repository
public interface EtlTaskRunRepository extends JpaRepository<EtlTaskRun, Long> {

    Optional<EtlTaskRun> findByRunId(String runId);

    /** 各状态 run 数（WP-15 Gauge：etl.run.state.count）。 */
    long countByStatus(String status);

    Optional<EtlTaskRun> findFirstByTaskIdAndIsCurrentTrue(String taskId);

    List<EtlTaskRun> findByTaskIdOrderByAttemptNoDesc(String taskId);

    /**
     * CAS 状态迁移：仅当当前状态仍为 {@code from} 时才写 {@code to}。
     *
     * @return 影响行数，0 表示并发失败或重复投递（幂等忽略），1 表示迁移成功
     */
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE EtlTaskRun r SET r.status = :to, r.updatedAt = CURRENT_TIMESTAMP "
            + "WHERE r.runId = :runId AND r.status = :from")
    int casStatus(@Param("runId") String runId, @Param("from") String from, @Param("to") String to);

    /**
     * 心跳/进度基线单调更新（T7、§7.3）：只在更新更晚的时间戳时写入。
     *
     * <p>自带写事务：{@code ProgressListener} 的心跳落点在事务之外（有意保持监听器不做长事务），
     * 若不加 {@code @Transactional} 则 {@code @Modifying} 直接抛 {@code TransactionRequiredException}，
     * 心跳一条都写不进去（§12.1 集成测试暴露）。
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskRun r SET r.lastProgressTs = :ts "
            + "WHERE r.runId = :runId AND (r.lastProgressTs IS NULL OR r.lastProgressTs < :ts)")
    int touchProgress(@Param("runId") String runId, @Param("ts") Date ts);

    /** 首次进入 RUNNING 记 started_at（仅当为空），保证幂等（T7）。 */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskRun r SET r.startedAt = :ts, r.lastProgressTs = :ts "
            + "WHERE r.runId = :runId AND r.startedAt IS NULL")
    int markStartedIfAbsent(@Param("runId") String runId, @Param("ts") Date ts);

    /** 进入 DISPATCHED 记录派工基线（T5/T21）。 */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskRun r SET r.dispatchedAt = :ts WHERE r.runId = :runId")
    int markDispatchedAt(@Param("runId") String runId, @Param("ts") Date ts);

    /** 进入终态：写 finished_at 与可选 error_summary（T10/T11/T13/T14/T15/T26 等）。 */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskRun r SET r.finishedAt = :ts, "
            + "r.errorSummary = COALESCE(:errorSummary, r.errorSummary) WHERE r.runId = :runId")
    int markFinished(@Param("runId") String runId, @Param("ts") Date ts,
                     @Param("errorSummary") String errorSummary);

    /** 自动重试：attempt_no+1 并排定退避到期时间（T8/T12）。 */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskRun r SET r.attemptNo = r.attemptNo + 1, r.nextAttemptAt = :nextAttemptAt "
            + "WHERE r.runId = :runId")
    int incrementAttemptAndSchedule(@Param("runId") String runId, @Param("nextAttemptAt") Date nextAttemptAt);

    /** 排队成因标记（T4/T6/T24），不触碰 status。 */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskRun r SET r.queuedReason = :reason WHERE r.runId = :runId")
    int markQueuedReason(@Param("runId") String runId, @Param("reason") String reason);

    /** T2 校验后写回分片总数（§7.1 副作用），不触碰 status。 */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskRun r SET r.shardTotal = :shardTotal WHERE r.runId = :runId")
    int setShardTotal(@Param("runId") String runId, @Param("shardTotal") int shardTotal);

    /**
     * 计数投影（§4.2）：把 checkpoint 聚合的<b>绝对值</b>覆写进 run 行的汇总列
     * （{@code shard_succeeded/failed/skipped} 与 {@code rows_*}）。
     *
     * <p>三条约束缺一不可：
     * <ul>
     *   <li>只写非 status 列，维持“status 唯一写路径是 {@link #casStatus}”的守卫（§11.3）；</li>
     *   <li><b>覆写而非累加</b>：至少一次投递下同一聚合会被投影多次，绝对值才幂等；</li>
     *   <li>不动 {@code shard_total}：它是 T2 写入的<b>分片计划数</b>，而非 checkpoint 派生的进度值；
     *       G-4（{@code GuardChain}）与收敛判定（{@code ConvergencePolicy}）都以它为基准，
     *       被投影覆盖会拿“已建分片数”污染“计划数”（无分片时直接归 0）。</li>
     * </ul>
     *
     * <p>也不刷 {@code updated_at}：该列只由状态写入方维护，否则终态 run 会因投影而
     * 违反 §12.2 校验 3（“终态后不得再被原地更新”）。
     *
     * <p>自带写事务：{@code ResultsListener} 的调用点在事务之外（与 {@link #touchProgress} 同理）。
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskRun r SET r.shardSucceeded = :shardSucceeded, r.shardFailed = :shardFailed, "
            + "r.shardSkipped = :shardSkipped, r.rowsIn = :rowsIn, r.rowsOut = :rowsOut, "
            + "r.rowsDropped = :rowsDropped, r.rowsDeduped = :rowsDeduped WHERE r.runId = :runId")
    int projectCounters(@Param("runId") String runId,
                        @Param("shardSucceeded") int shardSucceeded,
                        @Param("shardFailed") int shardFailed,
                        @Param("shardSkipped") int shardSkipped,
                        @Param("rowsIn") long rowsIn,
                        @Param("rowsOut") long rowsOut,
                        @Param("rowsDropped") long rowsDropped,
                        @Param("rowsDeduped") long rowsDeduped);

    /** 软取消标记（T27/D-18），不触碰 status。 */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskRun r SET r.cancelRequestedAt = :ts WHERE r.runId = :runId")
    int markCancelRequested(@Param("runId") String runId, @Param("ts") Date ts);

    /** 归档旧终态 run（T28/T29）：置 is_current=FALSE（→ SUPERSEDED 由 transition 完成 status 变更）。 */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskRun r SET r.isCurrent = FALSE WHERE r.runId = :runId")
    int clearCurrent(@Param("runId") String runId);

    // --- sweeper 候选查询（§6.5 / §7.9，均限 is_current 的非终态 run）---

    /** DISPATCHED 且派工基线早于 cutoff（DispatchTimeoutSweeper，T8/T9）。 */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE AND r.status = 'DISPATCHED' "
            + "AND r.dispatchedAt IS NOT NULL AND r.dispatchedAt < :cutoff")
    List<EtlTaskRun> findDispatchedTimedOut(@Param("cutoff") Date cutoff);

    /** RETRYING 且退避已到期（BackoffReleaseSweeper，T21）。 */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE AND r.status = 'RETRYING' "
            + "AND r.nextAttemptAt IS NOT NULL AND r.nextAttemptAt <= :now")
    List<EtlTaskRun> findRetryingDue(@Param("now") Date now);

    /** QUEUED 且驻留早于 cutoff（饥饿判定，T32；以 updated_at 作为入队时间代理）。 */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE AND r.status = 'QUEUED' "
            + "AND r.updatedAt < :cutoff")
    List<EtlTaskRun> findQueuedOlderThan(@Param("cutoff") Date cutoff);

    /** VALIDATED 且驻留早于 cutoff（内部错误，T30）。 */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE AND r.status = 'VALIDATED' "
            + "AND r.updatedAt < :cutoff")
    List<EtlTaskRun> findValidatedStuck(@Param("cutoff") Date cutoff);

    /** CREATED 且驻留早于 cutoff（校验链未走完，T31 重放；以 updated_at 作为创建时间代理）。 */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE AND r.status = 'CREATED' "
            + "AND r.updatedAt < :cutoff")
    List<EtlTaskRun> findCreatedStuck(@Param("cutoff") Date cutoff);

    /** RUNNING 且最近心跳早于 cutoff（卡死候选，T13；最终由 SweeperPolicy 叠 checkpoint 活动精判）。 */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE AND r.status = 'RUNNING' "
            + "AND (r.lastProgressTs IS NULL OR r.lastProgressTs < :cutoff)")
    List<EtlTaskRun> findRunningStalledCandidates(@Param("cutoff") Date cutoff);

    /** RUNNING 且取消请求早于 cutoff（ack 兜底候选，T16；须再叠 quiesce）。 */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE AND r.status = 'RUNNING' "
            + "AND r.cancelRequestedAt IS NOT NULL AND r.cancelRequestedAt < :cutoff")
    List<EtlTaskRun> findCancelTimedOutCandidates(@Param("cutoff") Date cutoff);

    /**
     * 待派工的 QUEUED run（{@code DispatchService.pump} 用，§7.2）：按入队时间升序取前 {@code pageable} 个，
     * 保证公平（先进先出）。仅取当前 run（{@code is_current=TRUE}）。
     */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE AND r.status = 'QUEUED' "
            + "ORDER BY r.updatedAt ASC")
    List<EtlTaskRun> findQueuedForDispatch(Pageable pageable);

    /**
     * 全量派工（WP-01 / P0-1）：除 QUEUED 外，DISPATCHED/RUNNING 且仍有 PENDING 分片的 run
     * 也必须继续补派；否则 30M 行任务只跑首批 permitsPerRun 片。
     */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE "
            + "AND r.status IN ('QUEUED','DISPATCHED','RUNNING') "
            + "AND EXISTS (SELECT c FROM EtlTaskCheckpoint c "
            + "            WHERE c.runId = r.runId AND c.status = 'PENDING') "
            + "ORDER BY r.updatedAt ASC")
    List<EtlTaskRun> findDispatchableRuns(Pageable pageable);

    /** 结果丢失兜底（WP-10 / P1-1）：收敛 sweeper 扫描的活跃 run。 */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE AND r.status IN ('DISPATCHED','RUNNING')")
    List<EtlTaskRun> findActiveRuns();

    /** 程序化读取当前 run（API 用）。 */
    Optional<EtlTaskRun> findFirstByTaskIdAndIsCurrentTrueOrderByAttemptNoDesc(String taskId);
}
