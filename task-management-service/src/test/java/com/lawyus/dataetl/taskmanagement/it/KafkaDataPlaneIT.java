package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlShardStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.DltMessage;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.ProgressMessage;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.ResultMessage;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.NEEDS_ATTENTION;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RUNNING;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * §12.1 矩阵第 11 行 + §7.3/§7.5/§7.6：三个控制面监听器在<b>真 Kafka broker</b> 上的往返。
 *
 * <p>消息由测试用 {@code KafkaTemplate} 真实投递，编排器侧真消费者组消费；断言的是"库里的状态"，
 * 不是方法调用——因此这条链覆盖了序列化、消费线程、事务提交、CAS 写库与幂等忽略的全部组合。
 *
 * <p>每个用例都 {@link #holdDispatchPumpLock()} 占住派工泵锁：打开 {@code newWorker} 后 5s 周期的泵
 * 会与断言赛跑，而用例自身不派工。
 */
class KafkaDataPlaneIT extends AbstractOrchestratorIT {

    private static final String CONSUMER_GROUP = "task-management-group";

    @BeforeAll
    static void createItTopics() {
        ItInfra.ensureItTopics();
    }

    @BeforeEach
    void sleepThePump() {
        holdDispatchPumpLock();
    }

    @Test
    @DisplayName("progress：首个心跳把 DISPATCHED 推到 RUNNING，并单调刷新心跳基线")
    void firstHeartbeat_movesDispatchedToRunning() {
        String taskId = seedTask("it_target_kp_progress");
        String runId = seedDispatchedRun(taskId, Duration.ofSeconds(1), 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.ASSIGNED, 0, 0, 0);
        backdateCheckpoint(runId, "shard-0", Duration.ofMinutes(5));
        long ts = System.currentTimeMillis();

        routing.setNewWorker(true);
        ProgressMessage msg = new ProgressMessage();
        msg.setTaskId(taskId);
        msg.setRunId(runId);
        msg.setShardId("shard-0");
        msg.setRowsIn(4);
        msg.setRowsOut(4);
        msg.setTimestamp(ts);
        kafkaTemplate.send(ItInfra.TOPIC_PROGRESS, taskId, toJson(msg));

        awaitStatus(runId, RUNNING);
        assertThat(runColumn(runId, "started_at")).isNotNull();
        assertThat(jdbc.queryForObject("SELECT last_progress_ts FROM etl_task_run WHERE run_id = ?",
                java.util.Date.class, runId).getTime()).isGreaterThanOrEqualTo(ts);
        assertThat(jdbc.queryForObject("SELECT updated_at FROM etl_task_checkpoint "
                        + "WHERE run_id = ? AND shard_id = 'shard-0'", java.util.Date.class, runId).getTime())
                .as("checkpoint 心跳刷新（卡死判定基线）").isGreaterThanOrEqualTo(ts);
        assertThat(taskStatus(taskId)).isEqualTo("RUNNING");
    }

    @Test
    @DisplayName("results：全分片 SUCCEEDED 且行数平 → 真 broker 往返后收敛 SUCCEEDED")
    void results_convergeRunToSucceeded() {
        String taskId = seedTask("it_target_kp_results");
        String runId = seedRun(taskId, RUNNING, 1, 2);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.SUCCEEDED, 10, 10, 0);
        seedCheckpoint(taskId, runId, "shard-1", EtlShardStatus.SUCCEEDED, 5, 5, 0);

        routing.setNewWorker(true);
        ResultMessage msg = new ResultMessage();
        msg.setTaskId(taskId);
        msg.setRunId(runId);
        msg.setShardId("shard-1");
        msg.setStatus("SUCCEEDED");
        kafkaTemplate.send(ItInfra.TOPIC_RESULTS, taskId, toJson(msg));

        awaitStatus(runId, SUCCEEDED);
        assertThat(taskStatus(taskId)).isEqualTo("SUCCEEDED");
        assertThat(runDateColumn(runId, "finished_at")).isNotNull();
        // 计数投影：run 行必等于 checkpoint 聚合（不投影则这些列停在 0，§12.2 校验 2 就恒空转）
        assertThat(runLongColumn(runId, "shard_succeeded")).isEqualTo(2L);
        assertThat(runLongColumn(runId, "shard_failed")).isZero();
        assertThat(runLongColumn(runId, "rows_in")).isEqualTo(15L);
        assertThat(runLongColumn(runId, "rows_out")).isEqualTo(15L);
        assertThat(runLongColumn(runId, "rows_dropped")).isZero();
        assertThat(runLongColumn(runId, "shard_total")).as("T2 写入的计划数不得被投影覆盖").isEqualTo(2L);
    }

    @Test
    @DisplayName("results：仍有分片未收敛时不许定局（消息被消费但状态不动）")
    void results_beforeAllShardsSettled_doesNotConverge() {
        String taskId = seedTask("it_target_kp_partial");
        String runId = seedRun(taskId, RUNNING, 1, 2);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.SUCCEEDED, 10, 10, 0);
        seedCheckpoint(taskId, runId, "shard-1", EtlShardStatus.ASSIGNED, 0, 0, 0);

        routing.setNewWorker(true);
        ResultMessage result = new ResultMessage();
        result.setTaskId(taskId);
        result.setRunId(runId);
        result.setShardId("shard-0");
        result.setStatus("SUCCEEDED");
        kafkaTemplate.send(ItInfra.TOPIC_RESULTS, taskId, toJson(result));

        // 哨兵：同 run 的一条 progress，被处理即证明 results 那条已被消费（避免"只是没到"的假阴性）
        ProgressMessage sentinel = new ProgressMessage();
        sentinel.setTaskId(taskId);
        sentinel.setRunId(runId);
        sentinel.setShardId("shard-0");
        long ts = System.currentTimeMillis();
        sentinel.setTimestamp(ts);
        kafkaTemplate.send(ItInfra.TOPIC_PROGRESS, taskId, toJson(sentinel));

        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jdbc.queryForObject("SELECT last_progress_ts FROM etl_task_run WHERE run_id = ?",
                        java.util.Date.class, runId).getTime()).isGreaterThanOrEqualTo(ts));
        assertThat(runStatus(runId)).isEqualTo(RUNNING);
        // 未定局也要投影：run 行已反映“1 个分片完事”，而不是停在初始 0 等终态才准。
        // 投影来自 results，哨兵来自 progress：两个 topic 不保证先后，故等它而不是假定已到达。
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() -> {
            assertThat(runLongColumn(runId, "shard_succeeded")).as("中间态计数已投影").isEqualTo(1L);
            assertThat(runLongColumn(runId, "rows_in")).isEqualTo(10L);
            assertThat(runLongColumn(runId, "rows_out")).isEqualTo(10L);
        });
        assertThat(runStatus(runId)).as("分片未全部落定，不得定局").isEqualTo(RUNNING);
    }

    @Test
    @DisplayName("dlt：行级毒（FATAL）→ NEEDS_ATTENTION，error_summary 留痕")
    void dltRowPoison_movesToNeedsAttention() {
        String taskId = seedTask("it_target_kp_dlt");
        String runId = seedRun(taskId, RUNNING, 1, 2);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.FAILED, 10, 0, 0, 0L, "FATAL");
        double before = counter("etl.dlt.received.total");

        routing.setNewWorker(true);
        DltMessage msg = new DltMessage();
        msg.setTaskId(taskId);
        msg.setRunId(runId);
        msg.setShardId("shard-0");
        msg.setErrorClass("FATAL");
        msg.setErrorDetail("unparseable row");
        msg.setRawPayload("{\"amount\":\"abc\"}");
        kafkaTemplate.send(ItInfra.TOPIC_DLT, taskId, toJson(msg));

        awaitStatus(runId, NEEDS_ATTENTION);
        assertThat(counter("etl.dlt.received.total") - before).isEqualTo(1.0);
        assertThat(runColumn(runId, "error_summary")).contains("DLT_ROW");
        assertThat(taskStatus(taskId)).isEqualTo("NEEDS_ATTENTION");
    }

    @Test
    @DisplayName("dlt：协议类（ILLEGAL_TRANSITION）只告警，不动业务状态（§7.6）")
    void dltProtocolViolation_doesNotTouchState() {
        String taskId = seedTask("it_target_kp_dlt_protocol");
        String runId = seedRun(taskId, RUNNING, 1, 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.SUCCEEDED, 10, 10, 0);
        double before = counter("etl.dlt.received.total");

        routing.setNewWorker(true);
        DltMessage protocol = new DltMessage();
        protocol.setTaskId(taskId);
        protocol.setRunId(runId);
        protocol.setShardId("shard-0");
        protocol.setErrorClass("ILLEGAL_TRANSITION");
        protocol.setErrorDetail("SUCCEEDED -> CREATED 不在迁移表");
        kafkaTemplate.send(ItInfra.TOPIC_DLT, taskId, toJson(protocol));

        // 计数涨 1 就等于是“这条已经被消费”，不需要哨兵也能排除“没收到”的假阴性
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(counter("etl.dlt.received.total") - before).isEqualTo(1.0));
        assertThat(runStatus(runId)).isEqualTo(RUNNING);
        assertThat(runDateColumn(runId, "finished_at")).isNull();
    }

    @Test
    @DisplayName("影子开关：newWorker=false 时三个监听器一律休眠，旧链路不受影响")
    void listenersAreAsleepWhenNewWorkerDisabled() {
        String taskId = seedTask("it_target_kp_shadow");
        String runId = seedDispatchedRun(taskId, Duration.ofSeconds(1), 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.ASSIGNED, 0, 0, 0);

        routing.setNewWorker(false);
        ProgressMessage msg = new ProgressMessage();
        msg.setTaskId(taskId);
        msg.setRunId(runId);
        msg.setShardId("shard-0");
        msg.setTimestamp(System.currentTimeMillis());
        kafkaTemplate.send(ItInfra.TOPIC_PROGRESS, taskId, toJson(msg));

        // 有界等待：休眠的新链路不该有任何写库动作
        org.awaitility.Awaitility.await().during(Duration.ofSeconds(3))
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.DISPATCHED));
    }

    @Test
    @DisplayName("毒消息不死循环：非法 JSON 被跳过后消费位点照常前进")
    void poisonPayload_doesNotStallTheContainer() throws Exception {
        String taskId = seedTask("it_target_kp_poison");
        String runId = seedRun(taskId, RUNNING, 1, 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.SUCCEEDED, 7, 7, 0);

        routing.setNewWorker(true);
        // 先投毒（不可解析），再投一条能定局的有效 results：后者被处理即证明位点已越过毒记录
        kafkaTemplate.send(ItInfra.TOPIC_RESULTS, taskId, "{ this is not json ").get(15, TimeUnit.SECONDS);
        ResultMessage good = new ResultMessage();
        good.setTaskId(taskId);
        good.setRunId(runId);
        good.setShardId("shard-0");
        good.setStatus("SUCCEEDED");
        kafkaTemplate.send(ItInfra.TOPIC_RESULTS, taskId, toJson(good)).get(15, TimeUnit.SECONDS);

        awaitStatus(runId, SUCCEEDED);
        assertThat(committedOffset(ItInfra.TOPIC_RESULTS))
                .as("消费位点至少越过 [毒记录, 有效记录] 两条").isGreaterThanOrEqualTo(2L);
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private void awaitStatus(String runId, EtlTaskStatus expected) {
        Awaitility.await().atMost(Duration.ofSeconds(30))
                .untilAsserted(() -> assertThat(runStatus(runId)).isEqualTo(expected));
    }

    /** 用同 run 的一条 progress 作为"消费已发生"的哨兵，避免用固定 sleep 判断"什么都没发生"。 */
    private void awaitSentinel(String runId, String taskId) {
        ProgressMessage sentinel = new ProgressMessage();
        sentinel.setTaskId(taskId);
        sentinel.setRunId(runId);
        sentinel.setShardId("shard-0");
        long ts = System.currentTimeMillis();
        sentinel.setTimestamp(ts);
        kafkaTemplate.send(ItInfra.TOPIC_PROGRESS, taskId, toJson(sentinel));
        Awaitility.await().atMost(Duration.ofSeconds(30)).untilAsserted(() ->
                assertThat(jdbc.queryForObject("SELECT last_progress_ts FROM etl_task_run WHERE run_id = ?",
                        java.util.Date.class, runId).getTime()).isGreaterThanOrEqualTo(ts));
    }

    private String toJson(Object payload) {
        try {
            return objectMapper.writeValueAsString(payload);
        } catch (Exception e) {
            throw new IllegalStateException("IT 消息序列化失败", e);
        }
    }

    private long committedOffset(String topic) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ItInfra.KAFKA_BOOTSTRAP);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "it-admin-offsets");
        try (AdminClient admin = AdminClient.create(props)) {
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    admin.listConsumerGroupOffsets(CONSUMER_GROUP).partitionsToOffsetAndMetadata()
                            .get(15, TimeUnit.SECONDS);
            return offsets.entrySet().stream()
                    .filter(e -> e.getKey().topic().equals(topic))
                    .mapToLong(e -> e.getValue().offset())
                    .sum();
        } catch (Exception e) {
            throw new IllegalStateException("读取消费位点失败（group=" + CONSUMER_GROUP + "）", e);
        }
    }
}
