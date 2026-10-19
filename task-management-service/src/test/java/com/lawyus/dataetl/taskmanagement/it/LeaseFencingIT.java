package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlShardStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.worker.checkpoint.CheckpointWriter;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WP-11 / Gate 2：lease + attempt 双 fencing。
 *
 * <p>场景：worker A 持有分片 lease 时 worker B 不能执行；lease 过期后 B 可接管，
 * 此时 A 的迟到 checkpoint 写入必须被拒绝（不能让旧 worker 覆盖新 worker 的计数/终态）。
 */
class LeaseFencingIT extends AbstractOrchestratorIT {

    @Test
    @DisplayName("lease 被接管后旧 worker 的迟到写入被拒，新 owner 写入生效")
    void leaseTakeoverRejectsStaleWriter() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_lease_fencing");
        String runId = seedRun(taskId, EtlTaskStatus.RUNNING, 1, 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.PENDING, 0, 0, 0);
        CheckpointWriter writer = new CheckpointWriter(jdbc);

        // A 拿到 lease；B 在有效期内拿不到，也不能写
        assertThat(writer.tryAcquireLease(taskId, runId, "shard-0", 1, "worker-a", 60)).isEqualTo(1);
        assertThat(writer.tryAcquireLease(taskId, runId, "shard-0", 1, "worker-b", 60)).isZero();
        assertThat(writer.upsert(taskId, runId, "shard-0", "RUNNING", "1", "1", 1,
                1, 1, 0, 0, 0, 0, "worker-b", null, null))
                .as("B 的迟到写入被 lease fencing 拒绝").isZero();
        assertThat(writer.upsert(taskId, runId, "shard-0", "RUNNING", "1", "1", 1,
                1, 1, 0, 0, 0, 0, "worker-a", null, null)).isEqualTo(1);

        // 模拟 A 崩溃、lease 过期：B 接管
        jdbc.update("UPDATE etl_task_checkpoint SET lease_until = now() - interval '1 second' "
                + "WHERE task_id = ? AND run_id = ? AND shard_id = ?", taskId, runId, "shard-0");
        assertThat(writer.tryAcquireLease(taskId, runId, "shard-0", 1, "worker-b", 60)).isEqualTo(1);

        assertThat(writer.upsert(taskId, runId, "shard-0", "SUCCEEDED", "1", "1", 1,
                1, 1, 0, 0, 0, 0, "worker-a", null, null))
                .as("A 在 B 接管后的迟到写入被拒").isZero();
        assertThat(writer.upsert(taskId, runId, "shard-0", "SUCCEEDED", "1", "1", 1,
                1, 1, 0, 0, 0, 0, "worker-b", null, null)).isEqualTo(1);

        assertThat(jdbc.queryForObject("SELECT lease_owner FROM etl_task_checkpoint "
                + "WHERE task_id = ? AND run_id = ? AND shard_id = ?", String.class, taskId, runId, "shard-0"))
                .isEqualTo("worker-b");
        assertThat(jdbc.queryForObject("SELECT status FROM etl_task_checkpoint "
                + "WHERE task_id = ? AND run_id = ? AND shard_id = ?", String.class, taskId, runId, "shard-0"))
                .isEqualTo("SUCCEEDED");
    }
}
