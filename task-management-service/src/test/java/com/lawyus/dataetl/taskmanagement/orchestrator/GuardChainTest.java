package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskDltRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GuardChainTest {

    private EtlTaskCheckpointRepository checkpointRepository;
    private EtlTaskDltRepository dltRepository;
    private GuardChain guardChain;

    @BeforeEach
    void setUp() {
        checkpointRepository = mock(EtlTaskCheckpointRepository.class);
        dltRepository = mock(EtlTaskDltRepository.class);
        guardChain = new GuardChain(checkpointRepository, dltRepository);
    }

    private EtlTaskRun runningRun() {
        EtlTaskRun run = EtlTaskRun.newRun("run-1", "task-1", 1);
        ReflectionTestUtils.setField(run, "status", "RUNNING");
        ReflectionTestUtils.setField(run, "shardTotal", 1);
        return run;
    }

    private RunAggregate balancedAggregate() {
        return new RunAggregate() {
            @Override public Long getPending() { return 0L; }
            @Override public Long getAssigned() { return 0L; }
            @Override public Long getRunning() { return 0L; }
            @Override public Long getSucceeded() { return 1L; }
            @Override public Long getFailed() { return 0L; }
            @Override public Long getSkipped() { return 0L; }
            @Override public Long getRowsIn() { return 10L; }
            @Override public Long getRowsOut() { return 10L; }
            @Override public Long getRowsDropped() { return 0L; }
            @Override public Long getRowsFiltered() { return 0L; }
            @Override public Long getRowsFailed() { return 0L; }
            @Override public Long getRowsDeduped() { return 0L; }
        };
    }

    @Test
    @DisplayName("G-7：存在未解决 DLT 时，即使行数平也不得进入 SUCCEEDED")
    void unresolvedDltBlocksSuccess() {
        when(checkpointRepository.aggregate("run-1")).thenReturn(balancedAggregate());
        when(dltRepository.countUnresolved("run-1")).thenReturn(1L);

        assertThatThrownBy(() -> guardChain.evaluate(runningRun(), EtlTaskStatus.SUCCEEDED,
                TransitionContext.builder("task-1").maxAttempts(3).build()))
                .isInstanceOf(GuardFailureException.class)
                .hasMessageContaining("G-7_DLT_CLEAN");
    }

    @Test
    @DisplayName("G-7：DLT 全部解决后允许 SUCCEEDED")
    void resolvedDltAllowsSuccess() {
        when(checkpointRepository.aggregate("run-1")).thenReturn(balancedAggregate());
        when(dltRepository.countUnresolved("run-1")).thenReturn(0L);

        assertThatCode(() -> guardChain.evaluate(runningRun(), EtlTaskStatus.SUCCEEDED,
                TransitionContext.builder("task-1").maxAttempts(3).build()))
                .doesNotThrowAnyException();
    }
}
