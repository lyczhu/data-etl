package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class CheckpointShardRangeResolverTest {

    private final CheckpointShardRangeResolver resolver = new CheckpointShardRangeResolver();
    private final EtlTaskRun run = EtlTaskRun.newRun("run-1", "task-1", 1);

    @Test
    @DisplayName("读取持久化的主键闭区间")
    void readsPersistedBounds() {
        EtlTaskCheckpoint pending = EtlTaskCheckpoint.newPending("task-1", "run-1", "shard-0", 5L, 50L);
        ShardBounds b = resolver.resolve(run, pending);
        assertThat(b.from()).isEqualTo(5L);
        assertThat(b.to()).isEqualTo(50L);
        assertThat(b.isEmpty()).isFalse();
    }

    @Test
    @DisplayName("无区间（非 JDBC 源）返回 empty")
    void emptyWhenNoBounds() {
        EtlTaskCheckpoint pending = EtlTaskCheckpoint.newPending("task-1", "run-1", "shard-0");
        assertThat(resolver.resolve(run, pending).isEmpty()).isTrue();
    }
}
