package com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

/** {@code etl_reconciliation_run} 仓储（Phase 2D）。 */
@Repository
public interface EtlReconciliationRunRepository extends JpaRepository<EtlReconciliationRun, Long> {

    Optional<EtlReconciliationRun> findByRunId(String runId);

    Page<EtlReconciliationRun> findByTaskIdOrderByIdDesc(String taskId, Pageable pageable);

    long countByStatus(String status);
}
