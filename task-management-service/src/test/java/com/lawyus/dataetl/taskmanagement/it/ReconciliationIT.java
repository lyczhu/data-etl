package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation.EtlReconciliationRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation.ReconciliationService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2D：真库（etl_it）对账闭环。
 *
 * <p>源表 20 行、目标表 20 个唯一 idempotency_key、checkpoint 四段平衡 → PASS；
 * 目标少一行 → MISMATCH；目标表不存在 → ERROR 且仍保留 task_id。
 */
class ReconciliationIT extends AbstractOrchestratorIT {

    @Autowired
    private ReconciliationService reconciliationService;

    @Test
    @DisplayName("源/目标各 20 行且四段平衡：PASS")
    void reconcilePass() {
        String target = targetTable();
        String taskId = seedTask(target);
        jdbc.update("UPDATE etl_task SET status = 'SUCCEEDED' WHERE task_id = ?", taskId);
        String runId = seedRun(taskId, EtlTaskStatus.SUCCEEDED, 1, 1);
        jdbc.update("UPDATE etl_task_run SET finished_at = now() WHERE run_id = ?", runId);
        seedCheckpoint(taskId, runId, 20, 20);

        createTarget(target, 20);

        EtlReconciliationRun result = reconciliationService.reconcile(runId, true);

        assertThat(result.getStatus()).isEqualTo(EtlReconciliationRun.PASS);
        assertThat(result.getTaskId()).isEqualTo(taskId);
        assertThat(result.getSourceRows()).isEqualTo(20L);
        assertThat(result.getTargetRows()).isEqualTo(20L);
        assertThat(result.getDistinctIdempotency()).isEqualTo(20L);
        assertThat(result.getExpectedTargetRows()).isEqualTo(20L);
        assertThat(result.getDiff()).isZero();
        assertThat(reconciliationService.find(runId)).isPresent();
        cleanup(taskId, target);
    }

    @Test
    @DisplayName("目标少一行：MISMATCH 且 diff=-1")
    void reconcileMismatch() {
        String target = targetTable();
        String taskId = seedTask(target);
        jdbc.update("UPDATE etl_task SET status = 'SUCCEEDED' WHERE task_id = ?", taskId);
        String runId = seedRun(taskId, EtlTaskStatus.SUCCEEDED, 1, 1);
        jdbc.update("UPDATE etl_task_run SET finished_at = now() WHERE run_id = ?", runId);
        seedCheckpoint(taskId, runId, 20, 20);
        createTarget(target, 19);

        EtlReconciliationRun result = reconciliationService.reconcile(runId, true);

        assertThat(result.getStatus()).isEqualTo(EtlReconciliationRun.MISMATCH);
        assertThat(result.getDiff()).isEqualTo(-1L);
        assertThat(result.getDetails()).contains("expected 20");
        cleanup(taskId, target);
    }

    @Test
    @DisplayName("目标表不存在：ERROR 且保留 task_id")
    void reconcileErrorKeepsTaskId() {
        String target = targetTable();
        String taskId = seedTask(target);
        jdbc.update("UPDATE etl_task SET status = 'SUCCEEDED' WHERE task_id = ?", taskId);
        String runId = seedRun(taskId, EtlTaskStatus.SUCCEEDED, 1, 1);
        jdbc.update("UPDATE etl_task_run SET finished_at = now() WHERE run_id = ?", runId);
        seedCheckpoint(taskId, runId, 20, 20);

        EtlReconciliationRun result = reconciliationService.reconcile(runId, true);

        assertThat(result.getStatus()).isEqualTo(EtlReconciliationRun.ERROR);
        assertThat(result.getTaskId()).isEqualTo(taskId);
        assertThat(result.getDetails()).contains("error");
        cleanup(taskId, target);
    }

    private void seedCheckpoint(String taskId, String runId, long rowsIn, long rowsOut) {
        jdbc.update("INSERT INTO etl_task_checkpoint (task_id, run_id, shard_id, status, shard_attempt, "
                        + "rows_in, rows_out, rows_filtered, rows_failed, rows_deduped) "
                        + "VALUES (?, ?, 'shard-0', 'SUCCEEDED', 1, ?, ?, 0, 0, 0)",
                taskId, runId, rowsIn, rowsOut);
    }

    private void createTarget(String table, int rows) {
        jdbc.execute("CREATE TABLE " + table
                + " (id BIGINT PRIMARY KEY, idempotency_key VARCHAR(128) UNIQUE)");
        for (int i = 1; i <= rows; i++) {
            jdbc.update("INSERT INTO " + table + " (id, idempotency_key) VALUES (?, ?)",
                    i, "it-key-" + i);
        }
    }

    private void cleanup(String taskId, String target) {
        jdbc.update("DELETE FROM etl_reconciliation_run WHERE task_id = ?", taskId);
        jdbc.execute("DROP TABLE IF EXISTS " + target);
    }

    private static String targetTable() {
        return "it_rec_" + UUID.randomUUID().toString().substring(0, 8);
    }
}
