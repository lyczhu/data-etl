package com.lawyus.dataetl.taskmanagement.orchestrator.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskAudit;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskAuditRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Phase 2B / INV-9：审计写入的字段、上下文与失败语义（同事务回滚由集成测试覆盖）。 */
class AuditWriterTest {

    private EtlTaskAuditRepository repository;
    private SimpleMeterRegistry registry;
    private AuditWriter writer;

    @BeforeEach
    void setUp() {
        repository = mock(EtlTaskAuditRepository.class);
        registry = new SimpleMeterRegistry();
        writer = new AuditWriter(repository, registry, new ObjectMapper());
    }

    @Test
    @DisplayName("新建 run 写 (none) -> CREATED")
    void recordCreated_writesNoneToCreated() {
        EtlTaskRun run = EtlTaskRun.newRun("r1", "t1", 1);

        writer.recordCreated(run, "system:submit");

        EtlTaskAudit saved = captureSaved();
        assertThat(saved.getTaskId()).isEqualTo("t1");
        assertThat(saved.getRunId()).isEqualTo("r1");
        assertThat(saved.getFromStatus()).isEqualTo(EtlTaskAudit.NONE);
        assertThat(saved.getToStatus()).isEqualTo("CREATED");
        assertThat(saved.getActor()).isEqualTo("system:submit");
    }

    @Test
    @DisplayName("成功迁移写 from/to/actor/reason/errorClass 与 context JSON")
    void record_writesTransitionFieldsAndContext() {
        EtlTaskRun run = EtlTaskRun.newRun("r1", "t1", 1);
        TransitionContext ctx = TransitionContext.builder("t1")
                .actor("api:command").reason("MANUAL_RETRY")
                .shardIds(List.of("shard-1")).maxAttempts(3).build();

        writer.record(run, EtlTaskStatus.NEEDS_ATTENTION, EtlTaskStatus.QUEUED, ctx);

        EtlTaskAudit saved = captureSaved();
        assertThat(saved.getFromStatus()).isEqualTo("NEEDS_ATTENTION");
        assertThat(saved.getToStatus()).isEqualTo("QUEUED");
        assertThat(saved.getActor()).isEqualTo("api:command");
        assertThat(saved.getReason()).isEqualTo("MANUAL_RETRY");
        assertThat(saved.getContext()).contains("MANUAL_RETRY").contains("shard-1");
    }

    @Test
    @DisplayName("审计写失败：指标 +1 且异常向上抛出（由事务回滚迁移）")
    void writeFailure_incrementsMetricAndPropagates() {
        when(repository.save(any(EtlTaskAudit.class))).thenThrow(new IllegalStateException("db down"));
        EtlTaskRun run = EtlTaskRun.newRun("r1", "t1", 1);

        assertThatThrownBy(() -> writer.recordCreated(run, "system:submit"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("db down");
        assertThat(registry.counter("etl.audit.write.failures").count()).isEqualTo(1.0);
    }

    private EtlTaskAudit captureSaved() {
        ArgumentCaptor<EtlTaskAudit> captor = ArgumentCaptor.forClass(EtlTaskAudit.class);
        verify(repository).save(captor.capture());
        return captor.getValue();
    }
}
