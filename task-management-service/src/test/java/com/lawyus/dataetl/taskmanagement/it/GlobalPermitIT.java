package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlShardStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.DispatchProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.TokenBucket;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WP-12 / Gate 2：多 orchestrator 实例共享同一份全局许可。
 *
 * <p>两个 {@link TokenBucket} 实例（等价于两个 JVM）都从 DB 的 ASSIGNED/RUNNING 计数读空位，
 * 一个实例占满后另一个也立即看到 0；分片终态后两者同时恢复 1，不会出现“每实例各 24”。
 */
class GlobalPermitIT extends AbstractOrchestratorIT {

    @Autowired
    private DispatchProperties dispatchProperties;

    @Autowired
    private EtlTaskCheckpointRepository checkpointRepository;

    @Test
    @DisplayName("两个 orchestrator 实例共享 DB 活跃分片计数，许可不会各算一份")
    void twoOrchestratorsShareGlobalPermits() {
        int capacity = dispatchProperties.getMaxConcurrentShards();
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_global_permit");
        String runId = seedRun(taskId, EtlTaskStatus.RUNNING, 1, capacity);
        for (int i = 0; i < capacity; i++) {
            seedCheckpoint(taskId, runId, "shard-" + i, EtlShardStatus.ASSIGNED, 0, 0, 0);
        }

        TokenBucket orchestratorA = new TokenBucket(dispatchProperties, checkpointRepository);
        TokenBucket orchestratorB = new TokenBucket(dispatchProperties, checkpointRepository);

        assertThat(orchestratorA.available()).as("实例 A 看到全局占满").isZero();
        assertThat(orchestratorB.available()).as("实例 B 也看到全局占满").isZero();

        jdbc.update("UPDATE etl_task_checkpoint SET status = 'SUCCEEDED' "
                + "WHERE run_id = ? AND shard_id = ?", runId, "shard-0");

        assertThat(orchestratorA.available()).isEqualTo(1);
        assertThat(orchestratorB.available()).isEqualTo(1);
        assertThat(orchestratorA.tryAcquireUpTo(5)).isEqualTo(1);
    }
}
