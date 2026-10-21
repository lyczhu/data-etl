package com.lawyus.dataetl.taskmanagement.service;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * 验证 {@code PUT /api/tasks/{id}} 的 status 入参已被封堵（orchestrator-design.md §9）：
 * {@code updateTask} 不得覆盖 {@code status} 与派生计数 {@code totalRecords/completedRecords}。
 */
class EtlTaskServiceSealStatusTest {

    private EtlTaskRepository repository;
    private EtlTaskService service;

    @BeforeEach
    void setUp() {
        repository = mock(EtlTaskRepository.class);
        service = new EtlTaskService();
        ReflectionTestUtils.setField(service, "etlTaskRepository", repository);

        EtlTask existing = new EtlTask();
        existing.setId(1L);
        existing.setTaskId("t-1");
        existing.setStatus("RUNNING");
        existing.setTotalRecords(100L);
        existing.setCompletedRecords(50L);
        existing.setDescription("old");
        existing.setDataSourceId(1L);
        existing.setSourceTableName("src");
        existing.setTargetTableName("tgt");
        when(repository.findById(1L)).thenReturn(Optional.of(existing));
        when(repository.save(any(EtlTask.class))).thenAnswer(inv -> inv.getArgument(0));
    }

    @Test
    @DisplayName("请求体携带的 status / 计数被忽略，仅非状态字段可更新")
    void statusAndCountersAreIgnored() {
        EtlTask payload = new EtlTask();
        payload.setStatus("HACKED_TERMINAL");
        payload.setTotalRecords(999L);
        payload.setCompletedRecords(999L);
        payload.setDescription("new");
        payload.setDataSourceId(1L);
        payload.setSourceTableName("src");
        payload.setTargetTableName("tgt");

        EtlTask result = service.updateTask(1L, payload);

        assertThat(result.getStatus()).isEqualTo("RUNNING");   // 未被请求体覆盖
        assertThat(result.getTotalRecords()).isEqualTo(100L);
        assertThat(result.getCompletedRecords()).isEqualTo(50L);
        // 非状态字段正常更新；源/目标定位保持不变
        assertThat(result.getDescription()).isEqualTo("new");
        assertThat(result.getDataSourceId()).isEqualTo(1L);
        assertThat(result.getTargetTableName()).isEqualTo("tgt");
    }

    @Test
    @DisplayName("创建后尝试改 source/target：拒绝（Phase 2E 冻结）")
    void immutableFieldsRejected() {
        EtlTask payload = new EtlTask();
        payload.setDescription("new");
        payload.setTargetTableName("other_table");

        assertThatThrownBy(() -> service.updateTask(1L, payload))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("immutable");
    }
}
