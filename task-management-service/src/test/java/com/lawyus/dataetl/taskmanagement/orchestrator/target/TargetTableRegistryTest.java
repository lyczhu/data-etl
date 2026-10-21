package com.lawyus.dataetl.taskmanagement.orchestrator.target;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Date;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Phase 2E：归属冲突与 dry-run 语义（真实 DROP 由 TargetTableCleanupIT 覆盖）。 */
class TargetTableRegistryTest {

    private EtlTargetTableRepository repository;
    private TargetTableRegistry registry;

    @BeforeEach
    void setUp() {
        repository = mock(EtlTargetTableRepository.class);
        registry = new TargetTableRegistry(repository, mock(JdbcTemplate.class),
                mock(DataSource.class), new TargetTableProperties());
    }

    @Test
    @DisplayName("同名表已归其它 task：assertAvailable/register 都抛冲突")
    void conflictRejected() {
        EtlTargetTable existing = EtlTargetTable.active(task("t1", "tgt"));
        when(repository.findByScope(null, "tgt")).thenReturn(Optional.of(existing));

        assertThatThrownBy(() -> registry.assertAvailable(null, "tgt", "t2"))
                .isInstanceOf(TargetTableConflictException.class)
                .hasMessageContaining("t1");
        assertThatThrownBy(() -> registry.register(task("t2", "tgt")))
                .isInstanceOf(TargetTableConflictException.class);
    }

    @Test
    @DisplayName("同一 task 重复登记幂等，只刷新 last_used_at")
    void sameTaskRegistrationIsIdempotent() {
        EtlTargetTable existing = EtlTargetTable.active(task("t1", "tgt"));
        existing.setLastUsedAt(new Date(0));
        when(repository.findByScope(null, "tgt")).thenReturn(Optional.of(existing));
        when(repository.save(any(EtlTargetTable.class))).thenAnswer(inv -> inv.getArgument(0));

        registry.register(task("t1", "tgt"));

        assertThat(existing.getLastUsedAt()).isAfter(new Date(0));
        verify(repository).save(existing);
    }

    @Test
    @DisplayName("dry-run 只返回候选，不标状态、不删表")
    void dryRunDoesNotPersist() {
        EtlTargetTable candidate = EtlTargetTable.active(task("t1", "tgt"));
        when(repository.findOrphanCandidates(any(Date.class))).thenReturn(List.of(candidate));
        when(repository.findByState(EtlTargetTable.ORPHANED)).thenReturn(List.of());

        TargetTableRegistry.CleanupReport report = registry.cleanup(true, 7);

        assertThat(report.dryRun()).isTrue();
        assertThat(report.candidates()).isEqualTo(1);
        assertThat(report.details()).containsExactly("tgt");
        verify(repository, never()).save(any());
    }

    private static EtlTask task(String taskId, String target) {
        EtlTask task = new EtlTask();
        task.setTaskId(taskId);
        task.setTargetTableName(target);
        return task;
    }
}
