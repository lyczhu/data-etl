package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.DispatchService;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.TokenBucket;
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §7.1 入口链 + §7.2 派工泵的真库/真 broker 验证：提交 → 建 run → V-1..V-4 → 分片计划落库 → 入队 → pump 派工。
 *
 * <p>分片计划由真 {@code JdbcSourceConnector.planShards} 产生（真连 IT 库读 MIN/MAX/COUNT），
 * 派工消息由测试自建消费者从真 topic 读回，用于核对"只带游标区间、不带数据行"的 §5.2 契约。
 */
class EntryChainIT extends AbstractOrchestratorIT {

    @Autowired
    private EtlTaskService etlTaskService;

    @Autowired
    private DispatchService dispatchService;

    @Autowired
    private TokenBucket tokenBucket;

    @BeforeEach
    void prepareTopicsAndSleepPump() {
        ItInfra.ensureItTopics();
        holdDispatchPumpLock();   // 用例自己决定何时 pump
    }

    @Test
    @DisplayName("提交（newWorker=true）：建 run + V-1..V-4 + 3 个带主键闭区间的 PENDING 分片 → QUEUED")
    void submitPlansShardsAndQueues() {
        long dataSourceId = stubDataSource();
        String taskId = submitTask(dataSourceId, ItInfra.SOURCE_TABLE);

        String runId = currentRunId(taskId);
        assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.QUEUED);
        assertThat(taskStatus(taskId)).as("INV-6 投影").isEqualTo("QUEUED");
        assertThat(jdbc.queryForObject("SELECT shard_total FROM etl_task_run WHERE run_id = ?",
                Integer.class, runId)).isEqualTo(3);
        // 20 行主键 1..20，chunk=8 → [1,7] [8,14] [15,20]
        assertThat(shardRanges(runId)).containsExactly("shard-0:1-7", "shard-1:8-14", "shard-2:15-20");
        assertThat(shardStatuses(runId).values()).containsOnly("PENDING");
        assertThat(runColumn(runId, "queued_reason")).isEqualTo("SCHEDULED");
    }

    @Test
    @DisplayName("提交（newWorker=false）：旧链路零改动，不建 run、状态仍为 new")
    void submitWithLegacyRoutingStaysUnchanged() {
        routing.setNewWorker(false);
        long dataSourceId = stubDataSource();
        EtlTask task = newTask(dataSourceId, ItInfra.SOURCE_TABLE);
        EtlTask created = etlTaskService.createTask(task);
        trackTask(created.getTaskId());

        assertThat(taskStatus(created.getTaskId())).isEqualTo("new");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM etl_task_run WHERE task_id = ?",
                Integer.class, created.getTaskId())).isZero();
    }

    @Test
    @DisplayName("V-1 失败（数据源解析不到）：T3 CREATED→FAILED(FATAL_CONFIG)，不产生分片计划")
    void unreachableDataSourceFailsAtAcceptance() {
        routing.setNewWorker(true);
        long unstubbed = seedDataSource();   // 没给 Feign 打桩 → 返回 null
        String taskId = submitTask(unstubbed, ItInfra.SOURCE_TABLE);

        String runId = currentRunId(taskId);
        assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.FAILED);
        assertThat(runColumn(runId, "error_summary")).contains("DATASOURCE_UNREACHABLE");
        assertThat(shardStatuses(runId)).isEmpty();
    }

    @Test
    @DisplayName("V-2 失败（源表不存在）：真 SPI 连库即抛，T3 FAILED")
    void missingSourceTableFailsValidation() {
        routing.setNewWorker(true);
        long dataSourceId = stubDataSource();
        String taskId = submitTask(dataSourceId, "it_table_that_does_not_exist");

        String runId = currentRunId(taskId);
        assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.FAILED);
        assertThat(runColumn(runId, "error_summary")).contains("SOURCE_UNREADABLE_OR_UNREGISTERED");
    }

    @Test
    @DisplayName("V-4 失败（空表 → 0 分片）：SHARD_PLAN_OUT_OF_RANGE，T3 FAILED")
    void emptySourceTableIsOutOfRange() {
        jdbc.execute("CREATE TABLE IF NOT EXISTS it_source_empty "
                + "(id BIGINT PRIMARY KEY, name VARCHAR(64))");
        routing.setNewWorker(true);
        long dataSourceId = stubDataSource();
        String taskId = submitTask(dataSourceId, "it_source_empty");

        String runId = currentRunId(taskId);
        assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.FAILED);
        assertThat(runColumn(runId, "error_summary")).contains("SHARD_PLAN_OUT_OF_RANGE(0)");
    }

    @Test
    @DisplayName("pump：3 片派成自描述派工消息，分片 PENDING→ASSIGNED，run 走 T5 → DISPATCHED")
    void pumpPublishesSelfDescribingDispatchCommands() {
        routing.setNewWorker(true);
        long dataSourceId = stubDataSource();
        String taskId = submitTask(dataSourceId, ItInfra.SOURCE_TABLE);
        String runId = currentRunId(taskId);
        double publishedBefore = counter("etl.dispatch.shard.published.total");

        dispatchService.pump();

        assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.DISPATCHED);
        assertThat(shardStatuses(runId).values()).containsOnly("ASSIGNED");
        assertThat(counter("etl.dispatch.shard.published.total") - publishedBefore).isEqualTo(3.0);
        assertThat(runColumn(runId, "dispatched_at")).isNotNull();

        List<String> payloads = drainDispatchMessagesFor(runId, 3);
        assertThat(payloads).hasSize(3);
        for (String payload : payloads) {
            assertThat(payload).contains("\"runId\":\"" + runId + "\"")
                    .contains("\"pkFrom\"").contains("\"pkTo\"")
                    .contains("\"dataSourceId\":" + dataSourceId)
                    .contains("\"sourceTable\":\"" + ItInfra.SOURCE_TABLE + "\"")
                    .contains("\"targetTable\":");
            // WP-06：派工消息不得携带明文连接/口令
            assertThat(payload).doesNotContain("sourceUrl").doesNotContain("password")
                    .doesNotContain("jdbc:");
            // §5.2：派工只带区间不带数据行 —— 一条命令远小于一个数据批
            assertThat(payload.length()).as("派工消息体积（字节）").isLessThan(2048);
        }
        assertThat(payloads).extracting(p -> p.replaceAll("(?s).*\"shardId\":\"([^\"]+)\".*", "$1"))
                .containsExactlyInAnyOrder("shard-0", "shard-1", "shard-2");

        // 幂等：已全部 ASSIGNED 后再次 pump，无 PENDING 可派 → 不重发、不重复迁移
        dispatchService.pump();
        assertThat(counter("etl.dispatch.shard.published.total") - publishedBefore).isEqualTo(3.0);
        assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.DISPATCHED);
    }

    @Test
    @DisplayName("T6 令牌不足：run 标 queued_reason=THROTTLED，分片保持 PENDING，状态不推进")
    void pumpWithoutPermitsThrottlesInsteadOfDispatching() {
        routing.setNewWorker(true);
        long dataSourceId = stubDataSource();
        String taskId = submitTask(dataSourceId, ItInfra.SOURCE_TABLE);
        String runId = currentRunId(taskId);

        // 把全局令牌桶抽干：下一轮 pump 申请不到许可 → T6 标 THROTTLED（不推进状态）
        int drained = tokenBucket.available();
        assertThat(tokenBucket.tryAcquire(drained)).isTrue();
        try {
            double throttledBefore = counter("etl.dispatch.throttled.total");
            dispatchService.pump();

            assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.QUEUED);
            assertThat(runColumn(runId, "queued_reason")).isEqualTo("THROTTLED");
            assertThat(shardStatuses(runId).values()).containsOnly("PENDING");
            assertThat(counter("etl.dispatch.throttled.total") - throttledBefore).isEqualTo(1.0);
        } finally {
            tokenBucket.release(drained);
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private EtlTask newTask(long dataSourceId, String sourceTable) {
        EtlTask task = new EtlTask();
        task.setDataSourceId(dataSourceId);
        task.setSourceTableName(sourceTable);
        task.setTargetTableName("it_e2e_placeholder");   // createTask 会改写为源表名 + taskId 前 8 位
        task.setDescription("§12.1 entry chain IT");
        return task;
    }

    /** 走真提交入口（{@code EtlTaskService.createTask}），返回 taskId。 */
    private String submitTask(long dataSourceId, String sourceTable) {
        routing.setNewWorker(true);
        EtlTask created = etlTaskService.createTask(newTask(dataSourceId, sourceTable));
        trackTask(created.getTaskId());
        return created.getTaskId();
    }

    private String currentRunId(String taskId) {
        return jdbc.queryForObject("SELECT run_id FROM etl_task_run WHERE task_id = ? ORDER BY id DESC LIMIT 1",
                String.class, taskId);
    }

    /** 用一次性消费者组从派工 topic 捞属于本 run 的消息（其它用例的残留按 runId 过滤掉）。 */
    private List<String> drainDispatchMessagesFor(String runId, int expected) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, ItInfra.KAFKA_BOOTSTRAP);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "it-verifier-" + UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        List<String> matched = new ArrayList<>();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
            // dispatch topic 是多分区（Gate1 #9），必须 assign 全部分区，不能只读 partition 0
            List<TopicPartition> partitions = consumer.partitionsFor(ItInfra.TOPIC_DISPATCH).stream()
                    .map(p -> new TopicPartition(ItInfra.TOPIC_DISPATCH, p.partition()))
                    .toList();
            consumer.assign(partitions);
            consumer.seekToBeginning(partitions);
            long deadline = System.currentTimeMillis() + 30_000L;
            while (matched.size() < expected && System.currentTimeMillis() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1_000));
                for (ConsumerRecord<String, String> record : records) {
                    if (record.value() != null && record.value().contains(runId)) {
                        matched.add(record.value());
                    }
                }
            }
        }
        return matched;
    }
}
