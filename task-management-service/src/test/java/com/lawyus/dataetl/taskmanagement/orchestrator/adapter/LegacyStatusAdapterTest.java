package com.lawyus.dataetl.taskmanagement.orchestrator.adapter;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.adapter.LegacyStatusAdapter.Intent;
import com.lawyus.dataetl.taskmanagement.orchestrator.adapter.LegacyStatusAdapter.LegacySignal;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class LegacyStatusAdapterTest {

    @Test
    @DisplayName("§11.1 completed→SUCCEEDED，failed→NEEDS_ATTENTION（无 errorClass 宁可多告）")
    void toTransition() {
        Intent done = LegacyStatusAdapter.toTransition(LegacySignal.COMPLETED);
        assertThat(done.to()).isEqualTo(EtlTaskStatus.SUCCEEDED);
        assertThat(done.errorClass()).isNull();

        Intent failed = LegacyStatusAdapter.toTransition(LegacySignal.FAILED);
        assertThat(failed.to()).isEqualTo(EtlTaskStatus.NEEDS_ATTENTION);
        assertThat(failed.errorClass()).isNull();
    }

    @Test
    @DisplayName("旧字面量解析：终态映射，非终态/未知返回 null")
    void parse() {
        assertThat(LegacyStatusAdapter.parseLegacyStatus("completed")).isEqualTo(LegacySignal.COMPLETED);
        assertThat(LegacyStatusAdapter.parseLegacyStatus("FAILED")).isEqualTo(LegacySignal.FAILED);
        assertThat(LegacyStatusAdapter.parseLegacyStatus("running")).isNull();
        assertThat(LegacyStatusAdapter.parseLegacyStatus("new")).isNull();
        assertThat(LegacyStatusAdapter.parseLegacyStatus(null)).isNull();
    }

    @Test
    @DisplayName("null 信号抛异常（协议违例不静默）")
    void nullSignalThrows() {
        assertThatThrownBy(() -> LegacyStatusAdapter.toTransition(null))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
