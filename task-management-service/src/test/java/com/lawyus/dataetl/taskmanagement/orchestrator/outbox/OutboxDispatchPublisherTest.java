package com.lawyus.dataetl.taskmanagement.orchestrator.outbox;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.ShardDispatchCommand;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

/** Phase 2C：outbox publisher 只入箱，不在此处发网络请求。 */
class OutboxDispatchPublisherTest {

    private OutboxRepository repository;
    private OutboxDispatchPublisher publisher;

    @BeforeEach
    void setUp() {
        repository = mock(OutboxRepository.class);
        publisher = new OutboxDispatchPublisher(repository, new ObjectMapper(), "it.dispatch");
    }

    @Test
    @DisplayName("按 topic + runId:shardId key 写入完整 JSON")
    void publish_insertsOutboxRow() {
        publisher.publish(command());

        verify(repository).insert(org.mockito.ArgumentMatchers.eq("it.dispatch"),
                org.mockito.ArgumentMatchers.eq("run-1:shard-1"),
                org.mockito.ArgumentMatchers.argThat(payload ->
                        payload.contains("\"taskId\":\"task-1\"")
                                && payload.contains("\"runId\":\"run-1\"")
                                && payload.contains("\"shardId\":\"shard-1\"")));
    }

    @Test
    @DisplayName("插入失败向上抛出，由 DispatchService 事务回滚")
    void publishFailurePropagates() {
        doThrow(new IllegalStateException("db down")).when(repository).insert(anyString(), anyString(), anyString());

        assertThatThrownBy(() -> publisher.publish(command()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("db down");
    }

    private static ShardDispatchCommand command() {
        ShardDispatchCommand command = new ShardDispatchCommand();
        command.setTaskId("task-1");
        command.setRunId("run-1");
        command.setShardId("shard-1");
        command.setAttemptNo(1);
        return command;
    }
}
