package com.lawyus.dataetl.taskmanagement.orchestrator.outbox;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.util.concurrent.SettableListenableFuture;

import java.sql.Timestamp;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Phase 2C：poller 的成功标记、失败退避与保留期清理。 */
class OutboxPollerTest {

    private OutboxRepository repository;
    private OutboxProperties properties;
    private KafkaTemplate<String, String> kafkaTemplate;
    private RedissonClient redisson;
    private RLock lock;
    private SimpleMeterRegistry registry;
    private OutboxPoller poller;

    @BeforeEach
    void setUp() {
        repository = mock(OutboxRepository.class);
        properties = new OutboxProperties();
        kafkaTemplate = mock(KafkaTemplate.class);
        redisson = mock(RedissonClient.class);
        lock = mock(RLock.class);
        when(redisson.getLock("etl:outbox:poller")).thenReturn(lock);
        when(lock.tryLock()).thenReturn(true);
        when(lock.isHeldByCurrentThread()).thenReturn(true);
        registry = new SimpleMeterRegistry();
        poller = new OutboxPoller(repository, properties, kafkaTemplate, redisson, registry);
    }

    @Test
    @DisplayName("投递成功：标 sent、指标 +1、解锁")
    void poll_marksSentOnSuccess() {
        when(repository.findDue(properties.getBatchSize()))
                .thenReturn(List.of(row(7, 0)));
        SettableListenableFuture<org.springframework.kafka.support.SendResult<String, String>> ok =
                new SettableListenableFuture<>();
        ok.set(null);
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(ok);

        poller.poll();

        verify(repository).markSent(7);
        verify(repository, never()).markFailed(org.mockito.ArgumentMatchers.anyLong(),
                org.mockito.ArgumentMatchers.anyInt(), org.mockito.ArgumentMatchers.anyLong(), anyString());
        verify(lock).unlock();
        assertThat(registry.counter("etl.outbox.sent").count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("投递失败：attempt+1、按退避重排、指标 +1")
    void poll_marksFailedWithBackoff() {
        when(repository.findDue(properties.getBatchSize()))
                .thenReturn(List.of(row(8, 0)));
        SettableListenableFuture<org.springframework.kafka.support.SendResult<String, String>> failed =
                new SettableListenableFuture<>();
        failed.setException(new RuntimeException("broker down"));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(failed);

        poller.poll();

        verify(repository).markFailed(org.mockito.ArgumentMatchers.eq(8L),
                org.mockito.ArgumentMatchers.eq(1),
                org.mockito.ArgumentMatchers.eq(1L),
                contains("broker down"));
        verify(repository, never()).markSent(org.mockito.ArgumentMatchers.anyLong());
        assertThat(registry.counter("etl.outbox.send.failures").count()).isEqualTo(1.0);
    }

    @Test
    @DisplayName("保留期清理按配置天数删除 sent 行")
    void cleanup_deletesSentRows() {
        properties.setRetentionDays(7);
        when(repository.deleteSentOlderThanDays(7)).thenReturn(4);

        poller.cleanupSentRows();

        verify(repository).deleteSentOlderThanDays(7);
        assertThat(registry.counter("etl.outbox.cleaned").count()).isEqualTo(4.0);
    }

    @Test
    @DisplayName("retention-days<=0：不清理")
    void cleanup_disabledWhenRetentionNonPositive() {
        properties.setRetentionDays(0);

        poller.cleanupSentRows();

        verify(repository, never()).deleteSentOlderThanDays(org.mockito.ArgumentMatchers.anyInt());
    }

    private static OutboxRow row(long id, int attempt) {
        return new OutboxRow(id, "it.dispatch", "run:shard", "{\"taskId\":\"t\"}", attempt,
                new Timestamp(System.currentTimeMillis()));
    }
}
