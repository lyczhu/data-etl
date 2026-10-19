package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.SettableListenableFuture;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class KafkaDispatchPublisherTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, String> kafkaTemplate = mock(KafkaTemplate.class);
    private final KafkaDispatchPublisher publisher =
            new KafkaDispatchPublisher(kafkaTemplate, new ObjectMapper(), "it.dispatch");

    private static ShardDispatchCommand command() {
        ShardDispatchCommand command = new ShardDispatchCommand();
        command.setTaskId("task-1");
        command.setRunId("run-1");
        command.setShardId("shard-0");
        return command;
    }

    @Test
    @DisplayName("派工 key = runId:shardId（同一 run 的不同分片可分散到不同分区）")
    void usesRunAndShardAsKey() {
        SettableListenableFuture<SendResult<String, String>> ack = new SettableListenableFuture<>();
        ack.set(null);
        when(kafkaTemplate.send(eq("it.dispatch"), eq("run-1:shard-0"), anyString())).thenReturn(ack);

        publisher.publish(command());

        verify(kafkaTemplate).send(eq("it.dispatch"), eq("run-1:shard-0"), anyString());
    }

    @Test
    @DisplayName("broker ack 失败 → 抛异常，调用方据此保持 PENDING 不标 ASSIGNED")
    void ackFailureThrows() {
        SettableListenableFuture<SendResult<String, String>> ack = new SettableListenableFuture<>();
        ack.setException(new RuntimeException("broker down"));
        when(kafkaTemplate.send(anyString(), anyString(), anyString())).thenReturn(ack);

        assertThatThrownBy(() -> publisher.publish(command()))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("dispatch publish failed");
    }
}
