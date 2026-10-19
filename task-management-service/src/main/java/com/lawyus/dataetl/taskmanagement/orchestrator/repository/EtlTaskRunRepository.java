package com.lawyus.dataetl.taskmanagement.orchestrator.repository;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

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

    /** 心跳/进度基线单调更新（T7、§7.3）：只在更新更晚的时间戳时写入。 */
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

    /** RUNNING 且最近心跳早于 cutoff（卡死候选，T13；最终由 SweeperPolicy 叠 checkpoint 活动精判）。 */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE AND r.status = 'RUNNING' "
            + "AND (r.lastProgressTs IS NULL OR r.lastProgressTs < :cutoff)")
    List<EtlTaskRun> findRunningStalledCandidates(@Param("cutoff") Date cutoff);

    /** RUNNING 且取消请求早于 cutoff（ack 兜底候选，T16；须再叠 quiesce）。 */
    @Query("SELECT r FROM EtlTaskRun r WHERE r.isCurrent = TRUE AND r.status = 'RUNNING' "
            + "AND r.cancelRequestedAt IS NOT NULL AND r.cancelRequestedAt < :cutoff")
    List<EtlTaskRun> findCancelTimedOutCandidates(@Param("cutoff") Date cutoff);
}
