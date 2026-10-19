package com.lawyus.dataetl.taskmanagement.orchestrator.repository;

import com.lawyus.dataetl.taskmanagement.orchestrator.RunAggregate;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;

/**
 * {@link EtlTaskCheckpoint} 仓储（orchestrator 侧只读聚合）。
 *
 * <p>写权矩阵（§4.1）规定 checkpoint 由 worker 直写，orchestrator 仅在此做：
 * <ul>
 *   <li>{@link #aggregate} — 收敛判定、guard 与计数投影的单次 SQL 聚合（走 {@code idx_checkpoint_run_status}）；</li>
 *   <li>{@link #touch} — progress 心跳仅刷新 {@code updated_at}，不改 shard 状态（§7.3）；</li>
 *   <li>{@link #countActiveShards} — quiesce（G-3）判定。</li>
 * </ul>
 */
@Repository
public interface EtlTaskCheckpointRepository extends JpaRepository<EtlTaskCheckpoint, Long> {

    /**
     * 单次聚合：按状态分桶计数 + 行数列求和，映射为 {@link RunAggregate}。
     * COALESCE 保证无分片时各列为 0（而非 null）；该聚合同时是
     * {@code etl_task_run} 计数列的唯一投影源（见 {@code RunCounterProjector}）。
     */
    @Query(value = "SELECT "
            + " COALESCE(SUM(CASE WHEN status = 'PENDING'   THEN 1 ELSE 0 END), 0) AS pending,   "
            + " COALESCE(SUM(CASE WHEN status = 'ASSIGNED'  THEN 1 ELSE 0 END), 0) AS assigned,  "
            + " COALESCE(SUM(CASE WHEN status = 'RUNNING'   THEN 1 ELSE 0 END), 0) AS running,   "
            + " COALESCE(SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END), 0) AS succeeded, "
            + " COALESCE(SUM(CASE WHEN status = 'FAILED'    THEN 1 ELSE 0 END), 0) AS failed,    "
            + " COALESCE(SUM(CASE WHEN status = 'SKIPPED'   THEN 1 ELSE 0 END), 0) AS skipped,   "
            + " COALESCE(SUM(rows_in), 0)      AS rowsIn,      "
            + " COALESCE(SUM(rows_out), 0)     AS rowsOut,     "
            + " COALESCE(SUM(rows_dropped), 0) AS rowsDropped, "
            + " COALESCE(SUM(rows_deduped), 0) AS rowsDeduped  "
            + "FROM etl_task_checkpoint WHERE run_id = :runId",
            nativeQuery = true)
    RunAggregate aggregate(@Param("runId") String runId);

    /** quiesce（G-3）：活跃分片（ASSIGNED/RUNNING）数量。0 表示可安全迁移到 RETRYING/取消。 */
    @Query("SELECT COUNT(c) FROM EtlTaskCheckpoint c "
            + "WHERE c.runId = :runId AND c.status IN ('ASSIGNED','RUNNING')")
    long countActiveShards(@Param("runId") String runId);

    /** 卡死判定（§7.9）：本 run 分片最近一次写入时间（无分片时为 null）。 */
    @Query("SELECT MAX(c.updatedAt) FROM EtlTaskCheckpoint c WHERE c.runId = :runId")
    Date maxUpdatedAt(@Param("runId") String runId);

    /**
     * progress 心跳仅刷新 updated_at，不改 shard 状态（§7.3）。
     *
     * <p>与 {@code EtlTaskRunRepository#touchProgress} 同理：调用方（ProgressListener）不在事务内，
     * 必须自带 {@code @Transactional}，否则 {@code @Modifying} 抛 {@code TransactionRequiredException}。
     */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskCheckpoint c SET c.updatedAt = :ts "
            + "WHERE c.runId = :runId AND c.shardId = :shardId")
    int touch(@Param("runId") String runId, @Param("shardId") String shardId, @Param("ts") Date ts);

    /**
     * 某 run 的待派工分片（PENDING），按 shardId 升序取前 {@code pageable} 个（§7.2）。
     * {@code DispatchService.pump} 逐片派工时用。
     */
    @Query("SELECT c FROM EtlTaskCheckpoint c WHERE c.runId = :runId AND c.status = 'PENDING' "
            + "ORDER BY c.shardId ASC")
    List<EtlTaskCheckpoint> findPendingShards(@Param("runId") String runId, Pageable pageable);

    /** 某 run 尚待派工的 PENDING 分片数（算申请令牌数 = min(pending, permitsPerRun)）。 */
    @Query("SELECT COUNT(c) FROM EtlTaskCheckpoint c WHERE c.runId = :runId AND c.status = 'PENDING'")
    long countPendingShards(@Param("runId") String runId);

    /**
     * 派工阶段 {@code PENDING → ASSIGNED}（§4.1 写权矩阵：这是编排器唯二合法的 checkpoint 写之一——
     * 另者为 {@code /retry}/{@code /abort} 的重置）。以 {@code status='PENDING'} 作 CAS 条件，
     * 只有真正从 PENDING 转出才返回 1，避免并发重复派工。
     */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskCheckpoint c SET c.status = 'ASSIGNED', c.updatedAt = CURRENT_TIMESTAMP "
            + "WHERE c.taskId = :taskId AND c.runId = :runId AND c.shardId = :shardId AND c.status = 'PENDING'")
    int markAssigned(@Param("taskId") String taskId, @Param("runId") String runId,
                     @Param("shardId") String shardId);
}
