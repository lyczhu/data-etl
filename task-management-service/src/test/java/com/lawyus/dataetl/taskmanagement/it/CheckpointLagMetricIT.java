package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlShardStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WP-15 回归：{@code etl.checkpoint.lag.seconds} 只应统计仍在推进的 run（{@code DISPATCHED/RUNNING}）。
 *
 * <p>终态 run 的 checkpoint 不会再更新；若把它们算进全局 MAX，系统空闲时 lag 会无限增长，
 * {@code EtlCheckpointLagHigh} 会在没有活跃任务时误报。
 */
class CheckpointLagMetricIT extends AbstractOrchestratorIT {

    @Test
    @DisplayName("终态 run 的旧 checkpoint 不计入全局 lag，活跃 run 的写入才计入")
    void globalCheckpointLagIgnoresTerminalRuns() {
        RunFixture terminal = seedRunFixture(EtlTaskStatus.FAILED, 1, 1);
        seedCheckpoint(terminal.taskId(), terminal.runId(), "shard-0",
                EtlShardStatus.FAILED, 10, 0, 0);
        backdateCheckpoint(terminal.runId(), "shard-0", Duration.ofHours(1));
        Date terminalUpdated = jdbc.queryForObject(
                "SELECT updated_at FROM etl_task_checkpoint WHERE run_id = ? AND shard_id = 'shard-0'",
                Date.class, terminal.runId());

        Date onlyTerminal = checkpointRepository.globalMaxUpdatedAt();
        if (onlyTerminal != null) {
            assertThat(onlyTerminal)
                    .as("终态 run 的 1h 前 checkpoint 不能成为全局 lag 的取值")
                    .isAfter(terminalUpdated);
        }

        RunFixture active = seedRunFixture(EtlTaskStatus.RUNNING, 1, 1);
        seedCheckpoint(active.taskId(), active.runId(), "shard-0",
                EtlShardStatus.ASSIGNED, 10, 0, 0);

        Date withActive = checkpointRepository.globalMaxUpdatedAt();
        assertThat(withActive).isNotNull();
        assertThat(System.currentTimeMillis() - withActive.getTime())
                .as("活跃 run 的 checkpoint 应立即反映到全局 lag")
                .isLessThan(60_000L);
    }
}
