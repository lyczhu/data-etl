package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * {@link RunCounterProjector} 的映射单测：断言聚合值按<b>绝对值</b>逐列覆写，
 * 以及"没有现成聚合值时先聚合"。列位次序（succeeded/failed/skipped, in/out/dropped/deduped）
 * 在这里固化，避免真库上投影错位（§12.2 校验 2 依赖它）。
 */
class RunCounterProjectorTest {

    private EtlTaskRunRepository runRepository;
    private EtlTaskCheckpointRepository checkpointRepository;
    private RunCounterProjector projector;

    @BeforeEach
    void setUp() {
        runRepository = mock(EtlTaskRunRepository.class);
        checkpointRepository = mock(EtlTaskCheckpointRepository.class);
        projector = new RunCounterProjector(runRepository, checkpointRepository);
    }

    private static RunAggregate agg(long succeeded, long failed, long skipped,
                                    long rowsIn, long rowsOut, long rowsDropped, long rowsDeduped) {
        return new RunAggregate() {
            @Override public Long getPending() { return 0L; }
            @Override public Long getAssigned() { return 0L; }
            @Override public Long getRunning() { return 0L; }
            @Override public Long getSucceeded() { return succeeded; }
            @Override public Long getFailed() { return failed; }
            @Override public Long getSkipped() { return skipped; }
            @Override public Long getRowsIn() { return rowsIn; }
            @Override public Long getRowsOut() { return rowsOut; }
            @Override public Long getRowsDropped() { return rowsDropped; }
            @Override public Long getRowsFiltered() { return rowsDropped; }
            @Override public Long getRowsFailed() { return 0L; }
            @Override public Long getRowsDeduped() { return rowsDeduped; }
        };
    }

    @Test
    @DisplayName("聚合值逐列绝对覆写到 run 行（含 rows_deduped，参与对账也要留痕）")
    void projectsAggregateAsAbsoluteValues() {
        projector.project("run-1", agg(3, 1, 2, 100, 90, 5, 7));

        verify(runRepository).projectCounters("run-1", 3, 1, 2, 100L, 90L, 5L, 7L);
        verify(checkpointRepository, never()).aggregate(anyString());
    }

    @Test
    @DisplayName("null 列按 0 投影（COALESCE 兜底之外的双保险，不写 null 进 NOT NULL 列）")
    void nullColumnsProjectAsZero() {
        RunAggregate withNulls = new RunAggregate() {
            @Override public Long getPending() { return null; }
            @Override public Long getAssigned() { return null; }
            @Override public Long getRunning() { return null; }
            @Override public Long getSucceeded() { return null; }
            @Override public Long getFailed() { return null; }
            @Override public Long getSkipped() { return null; }
            @Override public Long getRowsIn() { return null; }
            @Override public Long getRowsOut() { return null; }
            @Override public Long getRowsDropped() { return null; }
            @Override public Long getRowsFiltered() { return null; }
            @Override public Long getRowsFailed() { return null; }
            @Override public Long getRowsDeduped() { return null; }
        };

        projector.project("run-1", withNulls);

        verify(runRepository).projectCounters("run-1", 0, 0, 0, 0L, 0L, 0L, 0L);
    }

    @Test
    @DisplayName("无现成聚合值时先聚合再投影（终态快照路径）")
    void aggregatesWhenNoSnapshotGiven() {
        when(checkpointRepository.aggregate("run-1")).thenReturn(agg(2, 0, 0, 20, 20, 0, 0));

        projector.project("run-1");

        verify(checkpointRepository).aggregate("run-1");
        verify(runRepository).projectCounters("run-1", 2, 0, 0, 20L, 20L, 0L, 0L);
    }

    @Test
    @DisplayName("重复投影同一聚合 → 同一覆写值（至少一次语义下幂等，不累加）")
    void repeatedProjectionIsIdempotent() {
        RunAggregate a = agg(1, 1, 0, 50, 30, 5, 0);

        projector.project("run-1", a);
        projector.project("run-1", a);

        verify(runRepository, times(2)).projectCounters("run-1", 1, 1, 0, 50L, 30L, 5L, 0L);
    }
}
