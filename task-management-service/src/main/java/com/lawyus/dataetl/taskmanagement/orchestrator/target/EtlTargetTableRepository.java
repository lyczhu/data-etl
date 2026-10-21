package com.lawyus.dataetl.taskmanagement.orchestrator.target;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;
import java.util.Optional;

/** {@code etl_target_table} 仓储（Phase 2E）。 */
@Repository
public interface EtlTargetTableRepository extends JpaRepository<EtlTargetTable, Long> {

    @Query("SELECT t FROM EtlTargetTable t WHERE t.tableName = :table "
            + "AND ((:targetId IS NULL AND t.targetDataSourceId IS NULL) OR t.targetDataSourceId = :targetId)")
    Optional<EtlTargetTable> findByScope(@Param("targetId") Long targetId, @Param("table") String table);

    Page<EtlTargetTable> findByStateOrderByIdDesc(String state, Pageable pageable);

    List<EtlTargetTable> findByTaskId(String taskId);

    List<EtlTargetTable> findByState(String state);

    long countByState(String state);

    /** ACTIVE 且 task 已删除，或 task 已终态且超过 cutoff → 孤儿候选。 */
    @Query("SELECT t FROM EtlTargetTable t WHERE t.state = 'ACTIVE' AND NOT EXISTS ("
            + "SELECT 1 FROM EtlTask e WHERE e.taskId = t.taskId AND ("
            + "LOWER(e.status) NOT IN ('succeeded','failed','cancelled') "
            + "OR COALESCE(e.updatedAt, e.createdAt) >= :cutoff))")
    List<EtlTargetTable> findOrphanCandidates(@Param("cutoff") Date cutoff);
}
