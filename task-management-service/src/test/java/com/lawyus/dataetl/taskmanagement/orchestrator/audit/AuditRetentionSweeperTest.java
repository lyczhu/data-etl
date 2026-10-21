package com.lawyus.dataetl.taskmanagement.orchestrator.audit;

import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskAuditRepository;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/** Phase 2B：审计保留期清理的安全开关与 cutoff 计算。 */
class AuditRetentionSweeperTest {

    private EtlTaskAuditRepository repository;
    private AuditProperties properties;
    private RedissonClient redisson;
    private RLock lock;
    private SimpleMeterRegistry registry;
    private AuditRetentionSweeper sweeper;

    @BeforeEach
    void setUp() throws Exception {
        repository = mock(EtlTaskAuditRepository.class);
        properties = new AuditProperties();
        properties.setRetentionDays(180);
        redisson = mock(RedissonClient.class);
        lock = mock(RLock.class);
        when(redisson.getLock("etl:sweeper:audit-retention")).thenReturn(lock);
        when(lock.tryLock(0, 30, TimeUnit.SECONDS)).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        registry = new SimpleMeterRegistry();
        sweeper = new AuditRetentionSweeper(repository, properties, redisson, registry);
    }

    @Test
    @DisplayName("按 retention-days 计算 cutoff 并解锁")
    void sweep_deletesOlderThanCutoff() {
        when(repository.deleteOlderThan(any(Date.class))).thenReturn(3);

        long before = System.currentTimeMillis() - 181L * 24 * 3600 * 1000;
        sweeper.sweep();

        verify(repository).deleteOlderThan(org.mockito.ArgumentMatchers.argThat(cutoff ->
                cutoff.getTime() > before && cutoff.getTime() < System.currentTimeMillis()));
        verify(lock).unlock();
        assertThat(registry.counter("etl.audit.cleanup.deleted").count()).isEqualTo(3.0);
    }

    @Test
    @DisplayName("retention-days<=0：不清理（安全开关）")
    void sweep_disabledWhenRetentionNonPositive() {
        properties.setRetentionDays(0);

        sweeper.sweep();

        verify(repository, never()).deleteOlderThan(any(Date.class));
        verifyNoInteractions(redisson);
    }
}
