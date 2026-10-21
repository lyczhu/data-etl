package com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Phase 2D：夜间扫描的 run 选择与逐 run 调用（外部库细节由 ReconciliationIT 覆盖）。 */
class ReconciliationSchedulerTest {

    private ReconciliationService service;
    private ReconciliationProperties properties;
    private JdbcTemplate jdbc;
    private RedissonClient redisson;
    private RLock lock;
    private ReconciliationScheduler scheduler;

    @BeforeEach
    void setUp() {
        service = mock(ReconciliationService.class);
        properties = new ReconciliationProperties();
        jdbc = mock(JdbcTemplate.class);
        redisson = mock(RedissonClient.class);
        lock = mock(RLock.class);
        when(redisson.getLock("etl:sweeper:reconciliation")).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        scheduler = new ReconciliationScheduler(service, properties, jdbc, redisson);
    }

    @Test
    @DisplayName("扫描近 48h 终态 run 并逐个对账，最后解锁")
    void sweepReconcilesEachRun() {
        when(jdbc.queryForList(anyString(), eq(String.class), any(), any()))
                .thenReturn(List.of("r1", "r2"));

        scheduler.sweep();

        verify(service).reconcile("r1", false);
        verify(service).reconcile("r2", false);
        verify(lock).unlock();
    }

    @Test
    @DisplayName("enabled=false：不查库、不加锁")
    void disabledDoesNothing() {
        properties.setEnabled(false);

        scheduler.sweep();

        verify(jdbc, never()).queryForList(anyString(), eq(String.class), any(), any());
        verify(service, never()).reconcile(anyString(), org.mockito.ArgumentMatchers.anyBoolean());
        verify(redisson, never()).getLock(anyString());
    }
}
