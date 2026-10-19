package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.RunAggregate;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.DispatchService;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.ShardDispatchCommand;
import com.lawyus.dataetl.taskmanagement.orchestrator.kafka.dto.ResultMessage;
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import com.lawyus.dataetl.worker.WorkerApplication;
import com.lawyus.dataetl.worker.datasource.DataSourceResolver;
import com.lawyus.dataetl.worker.datasource.ResolvedDataSource;
import com.lawyus.dataetl.worker.pipeline.IdempotencyKeys;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * §12.1 的<b>最高保真用例</b>：真 orchestrator 上下文（同 JVM）+ 真 etl-worker 上下文 + 真 Kafka +
 * 真 PostgreSQL 三件套，跑一条完整的新链路：
 *
 * <pre>
 * createTask → T1 建 run → V-1..V-4（真连库 planShards）→ T2/T4 入队
 *   → DispatchService.pump 投 etl.task.dispatch.batch（只带主键闭区间）
 *   → 真 worker 消费：JDBC 流式读 → 转换 → 批写目标表 → 直写 checkpoint → 回报 results
 *   → ResultsListener 聚合重算 → T7/T10 收敛 SUCCEEDED
 * </pre>
 *
 * <p>断言落在<b>真实数据</b>上（目标表行数、checkpoint 计数、INV-6 投影、§12.2 四条一致性 SQL），
 * 而不只是状态字面量。
 */
class FullChainE2EIT extends AbstractOrchestratorIT {

    @Autowired
    private EtlTaskService etlTaskService;

    @Autowired
    private DispatchService dispatchService;

    private static ConfigurableApplicationContext workerContext;

    @BeforeAll
    static void startRealWorker() {
        ItInfra.assumeInfrastructureUpAndBootstrap();
        ItInfra.ensureItTopics();
        // 独立于测试上下文：真 worker 自带 Kafka 消费者组（it-etl-worker），offsets 从最新开始，
        // 不去回放本 JVM 里其它用例留在派工 topic 上的历史消息。
        workerContext = new SpringApplicationBuilder(WorkerApplication.class, WorkerTestConfig.class)
                .web(WebApplicationType.NONE)
                .run("--spring.config.location=classpath:/it-worker.yml",
                        "--spring.cloud.config.enabled=false",
                        "--spring.datasource.url=" + ItInfra.IT_DB_URL,
                        "--spring.datasource.hikari.maximum-pool-size=3",
                        "--spring.datasource.hikari.minimum-idle=1",
                        "--spring.kafka.bootstrap-servers=" + ItInfra.KAFKA_BOOTSTRAP,
                        "--spring.kafka.consumer.auto-offset-reset=latest",
                        "--etl.worker.consumer-group=" + ItInfra.WORKER_GROUP + "-chain",
                        "--etl.topics.dispatch-batch=" + ItInfra.TOPIC_DISPATCH,
                        "--etl.topics.progress=" + ItInfra.TOPIC_PROGRESS,
                        "--etl.topics.results=" + ItInfra.TOPIC_RESULTS,
                        "--etl.topics.dlt=" + ItInfra.TOPIC_DLT);
        awaitWorkerAssigned();
    }

    /**
     * 等消费组真正拿到分区后再放行派工：worker 以 latest 启动（隔离同 JVM 其它用例的历史消息），
     * 若 pump 早于 rebalance 完成，派工消息会被 latest 直接跳过，run 永远停在 DISPATCHED。
     */
    private static void awaitWorkerAssigned() {
        KafkaListenerEndpointRegistry registry =
                workerContext.getBean(KafkaListenerEndpointRegistry.class);
        Awaitility.await().atMost(Duration.ofSeconds(30)).until(() ->
                registry.getListenerContainers().stream().allMatch(FullChainE2EIT::hasAssignment));
    }

    private static boolean hasAssignment(MessageListenerContainer container) {
        if (!container.isRunning()) {
            return false;
        }
        try {
            Method method = container.getClass().getMethod("getAssignedPartitions");
            Object assigned = method.invoke(container);
            return assigned instanceof Collection<?> partitions && !partitions.isEmpty();
        } catch (ReflectiveOperationException unavailable) {
            return true;   // 老版本 Spring Kafka 无该 API 时退化为 running
        }
    }

    @AfterAll
    static void stopRealWorker() {
        if (workerContext != null) {
            workerContext.close();
            workerContext = null;
        }
    }

    /**
     * IT 里没有 data-source-service：派工消息只带 dataSourceId 后，worker 必须能解析出
     * 真源库连接。这里用 {@code @Primary} 替身覆盖远程解析，其余 worker 逻辑保持真实。
     */
    @Configuration
    static class WorkerTestConfig {
        @Bean
        @Primary
        DataSourceResolver itDataSourceResolver() {
            return new DataSourceResolver(new RestTemplateBuilder(), "http://localhost:8081") {
                @Override
                public ResolvedDataSource resolve(Long dataSourceId) {
                    return new ResolvedDataSource(dataSourceId, ItInfra.SOURCE_TYPE,
                            ItInfra.SOURCE_URL, ItInfra.PG_USER, ItInfra.PG_PASSWORD,
                            ItInfra.SOURCE_DRIVER);
                }
            };
        }
    }

    @BeforeEach
    void holdThePump() {
        holdDispatchPumpLock();   // 派工时机由用例掌控（同线程可重入）
    }

    @Test
    @DisplayName("全链路：提交 → 校验分片 → 派工 → 真 worker 搬 20 行 → 收敛 SUCCEEDED，一致性不变式全过")
    void fullChainMovesRealRowsAndConverges() {
        routing.setNewWorker(true);
        long dataSourceId = stubDataSource();
        EtlTask request = new EtlTask();
        request.setDataSourceId(dataSourceId);
        request.setSourceTableName(ItInfra.SOURCE_TABLE);
        request.setDescription("§12.1 full chain E2E");
        EtlTask created = etlTaskService.createTask(request);
        String taskId = created.getTaskId();
        trackTask(taskId);
        String targetTable = created.getTargetTableName();

        String runId = currentRunId(taskId);
        assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.QUEUED);
        assertThat(shardRanges(runId)).containsExactly("shard-0:1-7", "shard-1:8-14", "shard-2:15-20");

        dispatchService.pump();
        assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.DISPATCHED);
        assertThat(shardStatuses(runId).values()).containsOnly("ASSIGNED");

        // 真 worker：读源 → 写目标 → 直写 checkpoint → 回报 results；编排器聚合重算定局
        Awaitility.await().atMost(Duration.ofSeconds(120))
                .untilAsserted(() -> assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.SUCCEEDED));

        Integer moved = jdbc.queryForObject("SELECT COUNT(*) FROM " + targetTable, Integer.class);
        assertThat(moved).as("真数据搬运：目标表行数 = 源表行数").isEqualTo(ItInfra.SOURCE_ROWS);

        assertThat(shardStatuses(runId).values()).containsOnly("SUCCEEDED");
        RunAggregate aggregate = checkpointRepository.aggregate(runId);
        assertThat(aggregate.getRowsIn()).isEqualTo(ItInfra.SOURCE_ROWS);
        assertThat(aggregate.getRowsOut()).isEqualTo(ItInfra.SOURCE_ROWS);
        assertThat(aggregate.getRowsDropped()).isZero();
        assertThat(aggregate.isBalanced()).as("G-4 对账等式（§12.2 校验 2 的真实口径）").isTrue();

        // 计数投影：run 行必镜像 checkpoint 聚合，否则下面校验 2 的字面 SQL 会因两边同为 0 而空转通过
        assertThat(runLongColumn(runId, "shard_succeeded")).as("分片成功数投影到 run 行")
                .isEqualTo(aggregate.getSucceeded());
        assertThat(runLongColumn(runId, "shard_failed")).isEqualTo(aggregate.getFailed());
        assertThat(runLongColumn(runId, "shard_skipped")).isEqualTo(aggregate.getSkipped());
        assertThat(runLongColumn(runId, "rows_in")).isEqualTo(ItInfra.SOURCE_ROWS);
        assertThat(runLongColumn(runId, "rows_out")).isEqualTo(ItInfra.SOURCE_ROWS);
        assertThat(runLongColumn(runId, "rows_dropped")).isZero();
        assertThat(runLongColumn(runId, "rows_deduped")).isEqualTo(aggregate.getRowsDeduped());
        assertThat(runLongColumn(runId, "shard_total")).as("T2 写入的分片计划数不得被投影覆盖").isEqualTo(3L);

        assertThat(taskStatus(taskId)).as("INV-6").isEqualTo("SUCCEEDED");
        assertThat(runColumn(runId, "started_at")).isNotNull();
        assertThat(runColumn(runId, "dispatched_at")).isNotNull();
        assertThat(runDateColumn(runId, "finished_at")).isNotNull();

        assertDesignDocInvariantsHold(runId);
    }

    /**
     * Gate 1 · #2/#3：崩溃窗口“目标已提交、checkpoint 未写”。
     *
     * <p>先把 shard-0 的 7 行按 worker 的幂等键直接写进目标表（模拟 sink commit 成功），
     * checkpoint 仍为 PENDING（模拟随后进程崩溃）。重启/派工后 worker 从 pkFrom 重读整片，
     * 7 行全部命中 {@code ON CONFLICT DO NOTHING}：目标表行数不增、{@code rows_deduped=7}，
     * 四元对账等式仍平——这正是 v2.0 的“deduped 不入等式”会误判的场景。
     */
    @Test
    @DisplayName("崩溃重放：目标已提交、checkpoint 未写 → 幂等去重后行数不变，仍可收敛 SUCCEEDED")
    void crashAfterTargetCommitReplaysIdempotently() {
        routing.setNewWorker(true);
        long dataSourceId = stubDataSource();
        EtlTask request = new EtlTask();
        request.setDataSourceId(dataSourceId);
        request.setSourceTableName(ItInfra.SOURCE_TABLE);
        request.setDescription("Gate1 #2/#3 crash replay");
        EtlTask created = etlTaskService.createTask(request);
        String taskId = created.getTaskId();
        String targetTable = created.getTargetTableName();
        trackTask(taskId);

        try {
            String runId = currentRunId(taskId);
            assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.QUEUED);
            assertThat(shardStatuses(runId).values()).containsOnly("PENDING");

            // 目标表由 worker 的 buildCreateTable 建出；这里按同一形状预置 shard-0 的已提交数据
            jdbc.execute("CREATE TABLE " + targetTable + " ("
                    + "id BIGINT, name VARCHAR(64), amount NUMERIC(10, 2), note VARCHAR(120), "
                    + "created_at TIMESTAMP, idempotency_key VARCHAR(64) PRIMARY KEY)");
            for (long pk = 1; pk <= 7; pk++) {
                jdbc.update("INSERT INTO " + targetTable
                                + " (id, name, amount, note, created_at, idempotency_key) "
                                + "SELECT id, name, amount, note, created_at, ? FROM " + ItInfra.SOURCE_TABLE
                                + " WHERE id = ?",
                        IdempotencyKeys.of(taskId, ItInfra.SOURCE_TABLE, pk), pk);
            }
            assertThat(countRows(targetTable)).isEqualTo(7);

            dispatchService.pump();
            Awaitility.await().atMost(Duration.ofSeconds(120))
                    .untilAsserted(() -> assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.SUCCEEDED));

            assertThat(countRows(targetTable)).as("目标行数 = 源行数，无重复").isEqualTo(ItInfra.SOURCE_ROWS);
            assertThat(countDistinctIds(targetTable)).as("主键去重后仍等于源行数").isEqualTo(ItInfra.SOURCE_ROWS);

            RunAggregate aggregate = checkpointRepository.aggregate(runId);
            assertThat(aggregate.getRowsIn()).as("重放再次读入 shard-0 的 7 行").isEqualTo(ItInfra.SOURCE_ROWS);
            assertThat(aggregate.getRowsOut()).as("新增行 = 20 - 已提交的 7").isEqualTo(13L);
            assertThat(aggregate.getRowsDeduped()).as("幂等命中 7 行").isEqualTo(7L);
            assertThat(aggregate.getRowsFailed()).isZero();
            assertThat(aggregate.isBalanced()).as("四元等式：out + filtered + failed + deduped = in").isTrue();
            assertThat(shardStatuses(runId).values()).containsOnly("SUCCEEDED");

            assertThat(runLongColumn(runId, "rows_in")).isEqualTo(ItInfra.SOURCE_ROWS);
            assertThat(runLongColumn(runId, "rows_out")).isEqualTo(13L);
            assertThat(runLongColumn(runId, "rows_deduped")).isEqualTo(7L);
            assertThat(taskStatus(taskId)).isEqualTo("SUCCEEDED");
            assertDesignDocInvariantsHold(runId);
        } finally {
            jdbc.execute("DROP TABLE IF EXISTS " + targetTable);
        }
    }

    /**
     * Gate 1 · #4：重复 dispatch 与重复 results 的幂等性。
     *
     * <p>任务已 SUCCEEDED 后重发同一条 dispatch：worker 从 checkpoint 的 last_cursor 之后读，
     * 目标行数不增、checkpoint 计数不回退；再重发一条 results，终态与 finished_at 不动。
     */
    @Test
    @DisplayName("重复投递：重发 dispatch/results 后目标不重复、run 状态与计数不回退")
    void duplicateDispatchAndResultsAreIdempotent() {
        routing.setNewWorker(true);
        long dataSourceId = stubDataSource();
        EtlTask request = new EtlTask();
        request.setDataSourceId(dataSourceId);
        request.setSourceTableName(ItInfra.SOURCE_TABLE);
        request.setDescription("Gate1 #4 duplicate delivery");
        EtlTask created = etlTaskService.createTask(request);
        String taskId = created.getTaskId();
        String targetTable = created.getTargetTableName();
        trackTask(taskId);

        try {
            String runId = currentRunId(taskId);
            dispatchService.pump();
            Awaitility.await().atMost(Duration.ofSeconds(120))
                    .untilAsserted(() -> assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.SUCCEEDED));

            assertThat(countRows(targetTable)).isEqualTo(ItInfra.SOURCE_ROWS);
            Date finishedAt = runDateColumn(runId, "finished_at");
            Date shardUpdatedAt = checkpointUpdatedAt(runId, "shard-0");

            // 重发 shard-0 的 dispatch：worker 必须重新执行，但从 last_cursor=7 之后读到 0 行
            ShardDispatchCommand replay = new ShardDispatchCommand();
            replay.setTaskId(taskId);
            replay.setRunId(runId);
            replay.setAttemptNo(1);
            replay.setShardId("shard-0");
            replay.setPkFrom(1L);
            replay.setPkTo(7L);
            replay.setSourceTable(ItInfra.SOURCE_TABLE);
            replay.setTargetTable(targetTable);
            replay.setDataSourceId(dataSourceId);
            replay.setConnectorType("POSTGRES");
            kafkaTemplate.send(ItInfra.TOPIC_DISPATCH, runId + ":shard-0",
                    objectMapper.writeValueAsString(replay)).get(15, TimeUnit.SECONDS);

            Awaitility.await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(checkpointUpdatedAt(runId, "shard-0").after(shardUpdatedAt))
                            .as("worker 确实重新执行了重复 dispatch").isTrue());

            assertThat(countRows(targetTable)).as("重复 dispatch 不产生重复数据")
                    .isEqualTo(ItInfra.SOURCE_ROWS);
            assertThat(countDistinctIds(targetTable)).isEqualTo(ItInfra.SOURCE_ROWS);
            assertThat(shardStatus(runId, "shard-0")).isEqualTo("SUCCEEDED");
            assertThat(runLongColumn(runId, "rows_in")).isEqualTo(ItInfra.SOURCE_ROWS);
            assertThat(runLongColumn(runId, "rows_out")).isEqualTo(ItInfra.SOURCE_ROWS);
            assertThat(runLongColumn(runId, "rows_deduped")).isZero();
            assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.SUCCEEDED);
            assertThat(runDateColumn(runId, "finished_at")).isEqualTo(finishedAt);

            // 重复 results：已终态的 run 幂等忽略（不迁移、不重写 finished_at）
            long resultsOffset = committedOffset("task-management-group", ItInfra.TOPIC_RESULTS);
            ResultMessage stale = new ResultMessage();
            stale.setTaskId(taskId);
            stale.setRunId(runId);
            stale.setShardId("shard-0");
            stale.setStatus("SUCCEEDED");
            kafkaTemplate.send(ItInfra.TOPIC_RESULTS, taskId,
                    objectMapper.writeValueAsString(stale)).get(15, TimeUnit.SECONDS);

            Awaitility.await().atMost(Duration.ofSeconds(30))
                    .untilAsserted(() -> assertThat(committedOffset("task-management-group", ItInfra.TOPIC_RESULTS))
                            .as("重复 results 已被消费").isGreaterThan(resultsOffset));
            assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.SUCCEEDED);
            assertThat(taskStatus(taskId)).isEqualTo("SUCCEEDED");
            assertThat(runDateColumn(runId, "finished_at")).isEqualTo(finishedAt);
            assertThat(countRows(targetTable)).isEqualTo(ItInfra.SOURCE_ROWS);
        } catch (Exception e) {
            throw new IllegalStateException("duplicate delivery IT failed", e);
        } finally {
            jdbc.execute("DROP TABLE IF EXISTS " + targetTable);
        }
    }

    private int countRows(String table) {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
        return count == null ? 0 : count;
    }

    private int countDistinctIds(String table) {
        Integer count = jdbc.queryForObject("SELECT COUNT(DISTINCT id) FROM " + table, Integer.class);
        return count == null ? 0 : count;
    }

    private Date checkpointUpdatedAt(String runId, String shardId) {
        return jdbc.queryForObject("SELECT updated_at FROM etl_task_checkpoint WHERE run_id = ? AND shard_id = ?",
                Date.class, runId, shardId);
    }

    private long committedOffset(String group, String topic) {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ItInfra.KAFKA_BOOTSTRAP);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "it-replay-offsets");
        try (AdminClient admin = AdminClient.create(props)) {
            Map<TopicPartition, OffsetAndMetadata> offsets =
                    admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                            .get(15, TimeUnit.SECONDS);
            return offsets.entrySet().stream()
                    .filter(e -> e.getKey().topic().equals(topic))
                    .mapToLong(e -> e.getValue().offset())
                    .sum();
        } catch (Exception e) {
            throw new IllegalStateException("读取消费位点失败（group=" + group + "）", e);
        }
    }

    /**
     * §12.2 nightly 一致性校验的四条 SQL 在真库上必须返空。
     *
     * <p>校验 2 的字面形式比的是 {@code etl_task_run.rows_*}，所以先断言 {@code rows_in} 已非 0：
     * 否则“run 行恒为 0 → 等式两边同 0 → 永远返空”会伪装成通过（本轮补上计数投影前正是这个形态）。
     */
    private void assertDesignDocInvariantsHold(String runId) {
        assertThat(runLongColumn(runId, "rows_in"))
                .as("非空转守卫：校验 2 比的是 run 行列，它必须已被投影成真值").isPositive();
        assertThat(jdbc.queryForList("SELECT t.task_id FROM etl_task t JOIN etl_task_run r "
                        + "ON r.task_id = t.task_id AND r.is_current WHERE t.status <> r.status"))
                .as("校验 1（INV-6）：task.status 必等于 current run.status").isEmpty();
        assertThat(jdbc.queryForList("SELECT ru.run_id FROM etl_task_run ru JOIN ("
                        + "SELECT run_id, SUM(rows_in) rows_in, SUM(rows_out) rows_out, SUM(rows_dropped) rows_dropped, "
                        + "SUM(rows_deduped) rows_deduped FROM etl_task_checkpoint GROUP BY run_id) a "
                        + "ON a.run_id = ru.run_id "
                        + "WHERE ru.status = 'SUCCEEDED' AND ru.rows_out + ru.rows_dropped + ru.rows_deduped <> ru.rows_in"))
                .as("校验 2（INV-4）：SUCCEEDED 的 run 行数必平（含幂等命中）").isEmpty();
        assertThat(jdbc.queryForList("SELECT run_id FROM etl_task_run WHERE status IN "
                        + "('SUCCEEDED','FAILED','CANCELLED') AND updated_at > finished_at + interval '5 minutes'"))
                .as("校验 3（P2/INV-10）：终态 run 不得在 finished_at 之后被原地更新").isEmpty();
        assertThat(jdbc.queryForList("SELECT task_id FROM etl_task_run WHERE is_current AND status NOT IN "
                        + "('SUCCEEDED','FAILED','CANCELLED','SUPERSEDED') GROUP BY task_id HAVING COUNT(*) > 1"))
                .as("校验 4（INV-1）：同 task 不得有多个非终态当前 run").isEmpty();
    }

    private String currentRunId(String taskId) {
        List<String> ids = jdbc.queryForList("SELECT run_id FROM etl_task_run WHERE task_id = ? ORDER BY id DESC",
                String.class, taskId);
        assertThat(ids).isNotEmpty();
        return ids.get(0);
    }
}
