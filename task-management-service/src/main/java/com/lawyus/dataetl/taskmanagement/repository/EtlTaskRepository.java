package com.lawyus.dataetl.taskmanagement.repository;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface EtlTaskRepository extends JpaRepository<EtlTask, Long> {
    List<EtlTask> findByStatus(String status);
    EtlTask findByTaskId(String taskId);
}
