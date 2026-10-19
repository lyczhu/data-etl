package com.lawyus.dataetl.taskmanagement.repository;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface EtlTaskRepository extends JpaRepository<EtlTask, Long> {
    List<EtlTask> findByStatus(String status);
    EtlTask findByTaskId(String taskId);

    /**
     * INV-6：{@code etl_task.status} 恒等于 current run 的 status，只能由本方法（transition 内部）单向刷新，
     * 禁止反向写入。见 {@code docs/architecture/orchestrator-design.md §6.1 / §12.2 校验 1}。
     *
     * @return 影响行数（taskId 不存在时为 0）
     */
    @Modifying(clearAutomatically = true)
    @Query("UPDATE EtlTask t SET t.status = :status WHERE t.taskId = :taskId")
    int refreshDerivedStatus(@Param("taskId") String taskId, @Param("status") String status);
}
