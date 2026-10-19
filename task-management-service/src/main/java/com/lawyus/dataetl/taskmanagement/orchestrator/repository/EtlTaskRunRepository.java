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
}
