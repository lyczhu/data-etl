package com.lawyus.dataetl.taskmanagement.orchestrator.repository;

import com.lawyus.dataetl.taskmanagement.orchestrator.RunAggregate;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Date;

/**
 * {@link EtlTaskCheckpoint} 仓储（orchestrator 侧只读聚合）。
 *
 * <p>写权矩阵（§4.1）规定 checkpoint 由 worker 直写，orchestrator 仅在此做：
 * <ul>
 *   <li>{@link #aggregate} — 收敛判定与 guard 的单次 SQL 聚合（走 {@code idx_checkpoint_run_status}）；</li>
 *   <li>{@link #touch} — progress 心跳仅刷新 {@code updated_at}，不改 shard 状态（§7.3）；</li>
 *   <li>{@link #countActiveShards} — quiesce（G-3）判定。</li>
 * </ul>
 */
@Repository
public interface EtlTaskCheckpointRepository extends JpaRepository<EtlTaskCheckpoint, Long> {

    /**
     * 单次聚合：按状态分桶计数 + 行数列求和，映射为 {@link RunAggregate}。
     * COALESCE 保证无分片时各列为 0（而非 null）。
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
            + " COALESCE(SUM(rows_dropped), 0) AS rowsDropped  "
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

    /** progress 心跳仅刷新 updated_at，不改 shard 状态（§7.3）。 */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTaskCheckpoint c SET c.updatedAt = :ts "
            + "WHERE c.runId = :runId AND c.shardId = :shardId")
    int touch(@Param("runId") String runId, @Param("shardId") String shardId, @Param("ts") Date ts);
}
