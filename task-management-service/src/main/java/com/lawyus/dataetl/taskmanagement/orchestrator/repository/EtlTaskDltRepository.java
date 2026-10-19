package com.lawyus.dataetl.taskmanagement.orchestrator.repository;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskDlt;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * {@code etl_task_dlt} 只读仓储（worker 直写）：G-7 / {@code /failures} 用。
 */
@Repository
public interface EtlTaskDltRepository extends JpaRepository<EtlTaskDlt, Long> {

    /** 未解决 DLT 数（G-7：>0 阻止 SUCCEEDED）。 */
    @Query("SELECT COUNT(d) FROM EtlTaskDlt d "
            + "WHERE d.runId = :runId AND d.resolvedAt IS NULL AND d.abandoned = false")
    long countUnresolved(@Param("runId") String runId);

    /** WP-15：全局未解决 DLT 数（etl.dlt.unresolved.size）。 */
    @Query("SELECT COUNT(d) FROM EtlTaskDlt d WHERE d.resolvedAt IS NULL AND d.abandoned = false")
    long countUnresolvedGlobal();

    /** {@code /failures} 最近 100 条（原文按需脱敏/截断）。 */
    List<EtlTaskDlt> findTop100ByRunIdOrderByIdAsc(String runId);
}
