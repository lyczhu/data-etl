package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.target.EtlTargetTable;
import com.lawyus.dataetl.taskmanagement.orchestrator.target.EtlTargetTableRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.target.TargetTableConflictException;
import com.lawyus.dataetl.taskmanagement.orchestrator.target.TargetTableRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Phase 2E：真库登记、冲突与孤儿清理（默认 dry-run，false 时才 DROP）。 */
class TargetTableCleanupIT extends AbstractOrchestratorIT {

    @Autowired
    private TargetTableRegistry registry;

    @Autowired
    private EtlTargetTableRepository targetTableRepository;

    @Test
    @DisplayName("终态 task 超期：dry-run 不删；非 dry-run 删表并标 PURGED")
    void cleanupOnlyWhenNotDryRun() {
        String target = "it_cleanup_" + UUID.randomUUID().toString().substring(0, 8);
        String taskId = seedTask(target);
        jdbc.execute("CREATE TABLE " + target + " (id BIGINT)");
        EtlTask task = registryTask(taskId, target);
        registry.register(task);
        jdbc.update("UPDATE etl_task SET status = 'SUCCEEDED', updated_at = now() - interval '10 days' "
                + "WHERE task_id = ?", taskId);

        try {
            TargetTableRegistry.CleanupReport dry = registry.cleanup(true, 7);
            assertThat(dry.dryRun()).isTrue();
            assertThat(dry.details()).contains(target);
            assertThat(tableExists(target)).isTrue();
            assertThat(state(taskId)).isEqualTo(EtlTargetTable.ACTIVE);

            TargetTableRegistry.CleanupReport real = registry.cleanup(false, 7);
            assertThat(real.dropped()).isGreaterThanOrEqualTo(1);
            assertThat(tableExists(target)).isFalse();
            assertThat(state(taskId)).isEqualTo(EtlTargetTable.PURGED);
        } finally {
            jdbc.update("DELETE FROM etl_target_table WHERE task_id = ?", taskId);
            jdbc.execute("DROP TABLE IF EXISTS " + target);
        }
    }

    @Test
    @DisplayName("同一目标表登记给第二个 task：冲突")
    void duplicateRegistrationConflicts() {
        String target = "it_conflict_" + UUID.randomUUID().toString().substring(0, 8);
        String task1 = seedTask(target);
        String task2 = seedTask(target);
        registry.register(registryTask(task1, target));

        try {
            assertThatThrownBy(() -> registry.register(registryTask(task2, target)))
                    .isInstanceOf(TargetTableConflictException.class)
                    .hasMessageContaining(task1);
        } finally {
            jdbc.update("DELETE FROM etl_target_table WHERE task_id IN (?, ?)", task1, task2);
        }
    }

    private EtlTask registryTask(String taskId, String target) {
        EtlTask task = new EtlTask();
        task.setTaskId(taskId);
        task.setTargetTableName(target);
        return task;
    }

    private boolean tableExists(String table) {
        return jdbc.queryForObject("SELECT to_regclass(?)", String.class, table) != null;
    }

    private String state(String taskId) {
        return targetTableRepository.findByTaskId(taskId).get(0).getState();
    }
}
