package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.RunAggregate;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.DispatchService;
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import com.lawyus.dataetl.worker.WorkerApplication;
import com.lawyus.dataetl.worker.datasource.DataSourceResolver;
import com.lawyus.dataetl.worker.datasource.ResolvedDataSource;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
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

import java.time.Duration;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Gate 1 · #9 并发派工（方案 v2 WP-08 / 评审 §9.1 #9）：
 * 1 个任务 8 分片、2 个 worker 同组消费；分片并行执行，任务期间无 rebalance 双跑。
 *
 * <p>两个 worker 是同一 JVM 内的两个真实 Spring 上下文（{@link WorkerApplication}），共用
 * {@code etl-worker} 消费组。IT 的 dispatch topic 由 {@link ItInfra#DISPATCH_PARTITIONS} 建成多分区，
 * 否则同组第二个消费者拿不到分区，这条用例会退化成单 worker 顺序消费。
 *
 * <p>“无双跑”的可观察证据：任务前后 consumer-group 的成员/分区分配快照一致（未发生 rebalance），
 * 且最终 {@code rows_in == rows_out == 源行数}、{@code rows_deduped == 0}、目标表无重复主键。
 */
class TwoWorkerConcurrencyIT extends AbstractOrchestratorIT {

    private static final String WORKER_GROUP = ItInfra.WORKER_GROUP + "-two";
    private static final String CONCURRENCY_SOURCE_TABLE = "it_source_concurrency";
    private static final int CONCURRENCY_ROWS = 24;
    private static final int SHARDS = 8;

    @Autowired
    private EtlTaskService etlTaskService;

    @Autowired
    private DispatchService dispatchService;

    private static ConfigurableApplicationContext workerA;
    private static ConfigurableApplicationContext workerB;

    private int originalPermitsPerRun;
    private int originalShardTargetCount;

    @BeforeAll
    static void startTwoWorkers() {
        ItInfra.assumeInfrastructureUpAndBootstrap();
        ItInfra.ensureItTopics();
        workerA = startWorker("a");
        workerB = startWorker("b");
        awaitTwoWorkersStable();
    }

    @AfterAll
    static void stopWorkers() {
        if (workerA != null) {
            workerA.close();
            workerA = null;
        }
        if (workerB != null) {
            workerB.close();
            workerB = null;
        }
    }

    @BeforeEach
    void holdPumpAndSeed() {
        holdDispatchPumpLock();
        originalPermitsPerRun = dispatchProps.getPermitsPerRun();
        originalShardTargetCount = dispatchProps.getShardTargetCount();
        seedConcurrencySource();
    }

    @AfterEach
    void restore() {
        dispatchProps.setPermitsPerRun(originalPermitsPerRun);
        dispatchProps.setShardTargetCount(originalShardTargetCount);
        jdbc.execute("DROP TABLE IF EXISTS " + CONCURRENCY_SOURCE_TABLE);
    }

    @Test
    @DisplayName("8 分片 + 2 worker：同组分担 8 个分区，任务无 rebalance 双跑并收敛 SUCCEEDED")
    void eightShardsRunAcrossTwoWorkersWithoutRebalance() {
        routing.setNewWorker(true);
        dispatchProps.setShardTargetCount(SHARDS);
        dispatchProps.setPermitsPerRun(SHARDS);

        Map<String, Set<Integer>> before = assignmentSnapshot();
        assertThat(before).as("两个 worker 各拿到一部分分区").hasSize(2);
        assertThat(before.values()).allSatisfy(parts -> assertThat(parts).isNotEmpty());

        long dataSourceId = stubDataSource();
        com.lawyus.dataetl.taskmanagement.client.DataSource stub = dataSourceServiceClient.getDataSourceById(dataSourceId);
        assertThat(stub).as("Feign 替身必须命中（否则 V-2 会拿 null）").isNotNull();
        assertThat(rawJdbcCount(CONCURRENCY_SOURCE_TABLE)).as("worker 连接串必须能看到夹具表")
                .isEqualTo(CONCURRENCY_ROWS);
        EtlTask request = new EtlTask();
        request.setDataSourceId(dataSourceId);
        request.setSourceTableName(CONCURRENCY_SOURCE_TABLE);
        request.setDescription("Gate1 #9 two workers / eight shards");
        EtlTask created = etlTaskService.createTask(request);
        String taskId = created.getTaskId();
        String targetTable = created.getTargetTableName();
        trackTask(taskId);

        try {
            String runId = currentRunId(taskId);
            assertThat(shardStatuses(runId)).hasSize(SHARDS);
            assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.QUEUED);

            dispatchService.pump();
            Awaitility.await().atMost(Duration.ofSeconds(120))
                    .untilAsserted(() -> assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.SUCCEEDED));

            Map<String, Set<Integer>> after = assignmentSnapshot();
            assertThat(after).as("任务执行期间不得发生 rebalance（成员/分区分配快照不变）").isEqualTo(before);

            assertThat(countRows(targetTable)).isEqualTo(CONCURRENCY_ROWS);
            assertThat(countDistinctIds(targetTable)).isEqualTo(CONCURRENCY_ROWS);
            assertThat(shardStatuses(runId).values()).containsOnly("SUCCEEDED");

            RunAggregate aggregate = checkpointRepository.aggregate(runId);
            assertThat(aggregate.getRowsIn()).isEqualTo(CONCURRENCY_ROWS);
            assertThat(aggregate.getRowsOut()).isEqualTo(CONCURRENCY_ROWS);
            assertThat(aggregate.getRowsDeduped()).as("无 rebalance 双跑 → 无重复处理").isZero();
            assertThat(aggregate.getRowsFailed()).isZero();
            assertThat(aggregate.isBalanced()).isTrue();
            assertThat(taskStatus(taskId)).isEqualTo("SUCCEEDED");
        } finally {
            jdbc.execute("DROP TABLE IF EXISTS " + targetTable);
        }
    }

    private static ConfigurableApplicationContext startWorker(String suffix) {
        return new SpringApplicationBuilder(WorkerApplication.class, WorkerTestConfig.class)
                .web(WebApplicationType.NONE)
                .run("--spring.config.location=classpath:/it-worker.yml",
                        "--spring.cloud.config.enabled=false",
                        "--spring.application.name=etl-worker-it-" + suffix,
                        "--spring.datasource.url=" + ItInfra.IT_DB_URL,
                        "--spring.datasource.hikari.maximum-pool-size=3",
                        "--spring.datasource.hikari.minimum-idle=1",
                        "--spring.kafka.bootstrap-servers=" + ItInfra.KAFKA_BOOTSTRAP,
                        "--spring.kafka.consumer.auto-offset-reset=latest",
                        "--etl.worker.consumer-group=" + WORKER_GROUP,
                        "--etl.topics.dispatch-batch=" + ItInfra.TOPIC_DISPATCH,
                        "--etl.topics.progress=" + ItInfra.TOPIC_PROGRESS,
                        "--etl.topics.results=" + ItInfra.TOPIC_RESULTS,
                        "--etl.topics.dlt=" + ItInfra.TOPIC_DLT);
    }

    private static void awaitTwoWorkersStable() {
        Awaitility.await().atMost(Duration.ofSeconds(60)).untilAsserted(() -> {
            ConsumerGroupDescription group = describeGroup();
            assertThat(group.state()).isEqualTo(ConsumerGroupState.STABLE);
            assertThat(group.members()).hasSize(2);
            Set<Integer> allPartitions = new HashSet<>();
            for (MemberDescription member : group.members()) {
                Set<Integer> owned = partitionsOf(member);
                assertThat(owned).as("成员 " + member.consumerId() + " 必须拿到分区").isNotEmpty();
                allPartitions.addAll(owned);
            }
            assertThat(allPartitions).hasSize(ItInfra.DISPATCH_PARTITIONS);
        });
    }

    /** memberId → 该成员持有的 dispatch 分区号（升序），用于“无 rebalance”快照比较。 */
    private static Map<String, Set<Integer>> assignmentSnapshot() {
        ConsumerGroupDescription group = describeGroup();
        Map<String, Set<Integer>> snapshot = new TreeMap<>();
        for (MemberDescription member : group.members()) {
            snapshot.put(member.consumerId(), partitionsOf(member));
        }
        return snapshot;
    }

    private static Set<Integer> partitionsOf(MemberDescription member) {
        Set<Integer> partitions = new TreeSet<>();
        for (TopicPartition tp : member.assignment().topicPartitions()) {
            if (tp.topic().equals(ItInfra.TOPIC_DISPATCH)) {
                partitions.add(tp.partition());
            }
        }
        return partitions;
    }

    private static ConsumerGroupDescription describeGroup() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ItInfra.KAFKA_BOOTSTRAP);
        props.put(AdminClientConfig.CLIENT_ID_CONFIG, "it-worker-group-describe");
        try (AdminClient admin = AdminClient.create(props)) {
            return admin.describeConsumerGroups(java.util.List.of(WORKER_GROUP))
                    .all().get(15, TimeUnit.SECONDS).get(WORKER_GROUP);
        } catch (Exception e) {
            throw new IllegalStateException("读取消费组失败（group=" + WORKER_GROUP + "）", e);
        }
    }

    private String currentRunId(String taskId) {
        return jdbc.queryForObject("SELECT run_id FROM etl_task_run WHERE task_id = ? AND is_current",
                String.class, taskId);
    }

    private int countRows(String table) {
        Integer count = jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class);
        return count == null ? 0 : count;
    }

    /** 用 worker/connector 同样的 DriverManager 连接读表，排除“两个连接指向不同库”。 */
    private int rawJdbcCount(String table) {
        try (java.sql.Connection conn = java.sql.DriverManager.getConnection(
                ItInfra.IT_DB_URL, ItInfra.PG_USER, ItInfra.PG_PASSWORD);
             java.sql.Statement st = conn.createStatement();
             java.sql.ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + table)) {
            rs.next();
            return rs.getInt(1);
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException("raw JDBC 读表失败：" + table, e);
        }
    }

    private int countDistinctIds(String table) {
        Integer count = jdbc.queryForObject("SELECT COUNT(DISTINCT id) FROM " + table, Integer.class);
        return count == null ? 0 : count;
    }

    private void seedConcurrencySource() {
        jdbc.execute("DROP TABLE IF EXISTS " + CONCURRENCY_SOURCE_TABLE);
        jdbc.execute("CREATE TABLE " + CONCURRENCY_SOURCE_TABLE + " ("
                + "id BIGINT PRIMARY KEY, name VARCHAR(64), amount NUMERIC(10, 2), "
                + "note VARCHAR(120), created_at TIMESTAMP)");
        for (int i = 1; i <= CONCURRENCY_ROWS; i++) {
            jdbc.update("INSERT INTO " + CONCURRENCY_SOURCE_TABLE
                            + " (id, name, amount, note, created_at) VALUES (?, ?, ?, ?, now())",
                    (long) i, "concurrency-" + i, 10.50, "concurrency fixture " + i);
        }
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM " + CONCURRENCY_SOURCE_TABLE, Integer.class))
                .as("夹具源表必须可见（库：%s）",
                        jdbc.queryForObject("SELECT current_database()", String.class))
                .isEqualTo(CONCURRENCY_ROWS);
    }

    /** IT 里没有 data-source-service：两个 worker 都从同一个替身解析真源库。 */
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
}
