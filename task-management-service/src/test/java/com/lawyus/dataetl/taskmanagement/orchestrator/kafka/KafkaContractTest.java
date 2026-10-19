package com.lawyus.dataetl.taskmanagement.orchestrator.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.ShardDispatchCommand;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.DltMessage;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.ProgressMessage;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.ResultMessage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WP-18：控制面 Kafka 契约测试。
 *
 * <p>用 worker 侧真实 JSON 形状反序列化，防止生产端/消费端字段漂移；同时锁死
 * “派工消息不携带连接/口令”的安全约束（WP-06）。
 */
class KafkaContractTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("dispatch 契约：字段对齐 worker，且不含 url/username/password")
    void dispatchContract() throws Exception {
        String json = "{\"taskId\":\"t1\",\"runId\":\"r1\",\"attemptNo\":2,\"shardId\":\"shard-0\","
                + "\"pkFrom\":1,\"pkTo\":100,\"resumeCursor\":\"42\",\"connectorType\":\"POSTGRES\","
                + "\"dataSourceId\":7,\"targetDataSourceId\":8,"
                + "\"sourceTable\":\"src_orders\",\"targetTable\":\"tgt_orders\"}";

        ShardDispatchCommand cmd = objectMapper.readValue(json, ShardDispatchCommand.class);

        assertThat(cmd.getTaskId()).isEqualTo("t1");
        assertThat(cmd.getRunId()).isEqualTo("r1");
        assertThat(cmd.getAttemptNo()).isEqualTo(2);
        assertThat(cmd.getShardId()).isEqualTo("shard-0");
        assertThat(cmd.getPkFrom()).isEqualTo(1L);
        assertThat(cmd.getPkTo()).isEqualTo(100L);
        assertThat(cmd.getResumeCursor()).isEqualTo("42");
        assertThat(cmd.getDataSourceId()).isEqualTo(7L);
        assertThat(cmd.getTargetDataSourceId()).isEqualTo(8L);

        String serialized = objectMapper.writeValueAsString(cmd);
        assertThat(serialized).doesNotContain("password").doesNotContain("jdbc:")
                .doesNotContain("username");
    }

    @Test
    @DisplayName("results/progress/dlt 契约：worker 真实字段可被消费端解析")
    void workerMessagesContract() throws Exception {
        ResultMessage result = objectMapper.readValue("{\"taskId\":\"t1\",\"runId\":\"r1\","
                + "\"shardId\":\"shard-0\",\"status\":\"SUCCEEDED\",\"rowsIn\":20,\"rowsOut\":18,"
                + "\"rowsDropped\":0,\"rowsDeduped\":2,\"errorClass\":null,\"errorDetail\":null}",
                ResultMessage.class);
        assertThat(result.getRowsIn()).isEqualTo(20);
        assertThat(result.getRowsOut()).isEqualTo(18);
        assertThat(result.getRowsDeduped()).isEqualTo(2);
        assertThat(result.getStatus()).isEqualTo("SUCCEEDED");

        ProgressMessage progress = objectMapper.readValue("{\"taskId\":\"t1\",\"runId\":\"r1\","
                + "\"shardId\":\"shard-0\",\"rowsIn\":5,\"rowsOut\":5,\"timestamp\":123456789}",
                ProgressMessage.class);
        assertThat(progress.getShardId()).isEqualTo("shard-0");
        assertThat(progress.getRowsIn()).isEqualTo(5);
        assertThat(progress.getTimestamp()).isEqualTo(123456789L);

        DltMessage dlt = objectMapper.readValue("{\"taskId\":\"t1\",\"runId\":\"r1\","
                + "\"shardId\":\"shard-0\",\"errorClass\":\"FATAL\",\"errorDetail\":\"bad row\","
                + "\"rawPayload\":\"{}\",\"kafkaOffset\":3}", DltMessage.class);
        assertThat(dlt.getErrorClass()).isEqualTo("FATAL");
        assertThat(dlt.getRawPayload()).isEqualTo("{}");
        assertThat(dlt.getKafkaOffset()).isEqualTo(3L);
    }
}
