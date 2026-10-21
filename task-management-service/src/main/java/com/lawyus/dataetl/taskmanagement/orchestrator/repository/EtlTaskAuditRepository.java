package com.lawyus.dataetl.taskmanagement.orchestrator.repository;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskAudit;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

/**
 * {@code etl_task_audit} 仓储（Phase 2B / INV-9）。
 *
 * <p>写入口只有 {@code AuditWriter}；本仓储只提供分页查询与保留期清理。
 */
@Repository
public interface EtlTaskAuditRepository extends JpaRepository<EtlTaskAudit, Long> {

    Page<EtlTaskAudit> findByTaskIdOrderByIdDesc(String taskId, Pageable pageable);

    long countByTaskId(String taskId);

    /** 保留期清理：删除早于 cutoff 的审计，返回删除行数。 */
    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("DELETE FROM EtlTaskAudit a WHERE a.createdAt < :cutoff")
    int deleteOlderThan(@Param("cutoff") Date cutoff);
}
