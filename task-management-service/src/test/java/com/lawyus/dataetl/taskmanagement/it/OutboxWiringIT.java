package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.DispatchPublisher;
import com.lawyus.dataetl.taskmanagement.orchestrator.outbox.OutboxDispatchPublisher;
import com.lawyus.dataetl.taskmanagement.orchestrator.outbox.OutboxPoller;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2C：{@code etl.outbox.enabled=true} 时 publisher/poller 的条件装配正确，且不产生双 publisher。
 */
@SpringBootTest(properties = {"etl.outbox.enabled=true", "etl.outbox.poll-interval-ms=3600000"})
class OutboxWiringIT extends AbstractOrchestratorIT {

    @Autowired
    private DispatchPublisher dispatchPublisher;

    @Autowired
    private OutboxPoller outboxPoller;

    @Test
    @DisplayName("outbox 开启：DispatchPublisher 为 Outbox 实现，poller 已装配")
    void outboxEnabledWiresOutboxBeans() {
        assertThat(dispatchPublisher).isInstanceOf(OutboxDispatchPublisher.class);
        assertThat(outboxPoller).isNotNull();
    }
}
