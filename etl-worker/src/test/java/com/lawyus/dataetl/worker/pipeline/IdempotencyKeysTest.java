package com.lawyus.dataetl.worker.pipeline;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class IdempotencyKeysTest {

    @Test
    @DisplayName("键 = sha256(taskId|sourceTable|pk)，同输入稳定、不同 pk 不同")
    void stableAndDistinct() {
        String a = IdempotencyKeys.of("task-1", "orders", 42);
        String b = IdempotencyKeys.of("task-1", "orders", 42);
        String c = IdempotencyKeys.of("task-1", "orders", 43);

        assertThat(a).isEqualTo(b).hasSize(64).matches("[0-9a-f]{64}");
        assertThat(c).isNotEqualTo(a);
    }

    @Test
    @DisplayName("键不含 runId：同一 task 的重复 run 必须命中同一目标行")
    void independentOfRun() {
        // 方法签名刻意不接收 runId；这里固化行为，防止后续“为了隔离 run”把 runId 加进键。
        assertThat(IdempotencyKeys.of("task-1", "orders", 42))
                .isEqualTo(IdempotencyKeys.of("task-1", "orders", 42));
    }

    @Test
    @DisplayName("withKey 追加 idempotency_key，不覆盖源表同名列")
    void withKeyAppendsWithoutOverwrite() {
        Map<String, Object> cols = new LinkedHashMap<>();
        cols.put("id", 1L);
        SourceRecord record = new SourceRecord("shard-0", 1L, cols);

        SourceRecord keyed = IdempotencyKeys.withKey(record, "abc");
        assertThat(keyed.columns()).containsEntry(IdempotencyKeys.COLUMN, "abc")
                .containsEntry("id", 1L);

        SourceRecord existing = new SourceRecord("shard-0", 1L, Map.of(
                "id", 1L, IdempotencyKeys.COLUMN, "source-key"));
        assertThat(IdempotencyKeys.withKey(existing, "ignored").columns())
                .containsEntry(IdempotencyKeys.COLUMN, "source-key");
    }
}
