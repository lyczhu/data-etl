package com.lawyus.dataetl.taskmanagement.it;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.TaskManagementApplication;
import com.lawyus.dataetl.taskmanagement.client.DataSourceServiceClient;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlShardStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.DispatchProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.OrchestratorProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.TokenBucket;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * §12.1 集成测试基类：<b>真</b> PostgreSQL（库 {@code etl_it}）+ 真 Kafka + 真 Redis（Redisson 锁），
 * 起完整的 {@link TaskManagementApplication} 上下文——真 {@link OrchestratorService}、真 GuardChain、
 * 真四个 sweeper、真三个监听器、真令牌桶，不打桩任何编排逻辑。
 *
 * <p>仅有的两处替身：
 * <ul>
 *   <li>{@link DataSourceServiceClient}（Feign）被 {@code @MockBean} 替换 —— 现网 data-source-service
 *       不在 IT 依赖里；返回的 {@code DataSource} 指向真实 PG，因此 V-2 的 {@code planShards} 仍是真连库查询；</li>
 *   <li>超时阈值在 {@code application-it.yml} 里调成 100d，使 {@code @EnableScheduling} 起的周期扫描
 *       在用例执行期间一律不命中；要验证 sweeper 时由用例临时改小并直接调用 {@code sweep()}。</li>
 * </ul>
 *
 * <p>状态种子一律走 SQL（测试夹具），生产写路径仍是唯一的 {@code OrchestratorService.transition()}：
 * 实体故意不暴露 {@code setStatus}，测试也没权限绕过它去"造"迁移。
 */
@Tag("it")
@ActiveProfiles("it")
@SpringBootTest(classes = TaskManagementApplication.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE)
public abstract class AbstractOrchestratorIT {

    protected static final String DATASOURCE_NAME_PREFIX = "it-ds-";
    /** 与 {@code DispatchService.LOCK_KEY} 同一把锁：测试线程占住它，5s 周期的泵（另一线程）就 tryLock 失败。 */
    protected static final String DISPATCH_PUMP_LOCK = "etl:sweeper:dispatch-pump";

    @DynamicPropertySource
    static void itProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", () -> ItInfra.IT_DB_URL);
        registry.add("spring.kafka.bootstrap-servers", () -> ItInfra.KAFKA_BOOTSTRAP);
        registry.add("spring.redis.host", () -> ItInfra.REDIS_HOST);
        registry.add("spring.redis.port", () -> String.valueOf(ItInfra.REDIS_PORT));
        registry.add("etl.topics.dispatch-batch", () -> ItInfra.TOPIC_DISPATCH);
        registry.add("etl.topics.progress", () -> ItInfra.TOPIC_PROGRESS);
        registry.add("etl.topics.results", () -> ItInfra.TOPIC_RESULTS);
        registry.add("etl.topics.dlt", () -> ItInfra.TOPIC_DLT);
    }

    @BeforeAll
    static void bootstrapInfrastructure() {
        ItInfra.assumeInfrastructureUpAndBootstrap();
    }

    @Autowired
    protected JdbcTemplate jdbc;

    @Autowired
    protected OrchestratorService orchestrator;

    @Autowired
    protected EtlTaskRunRepository runRepository;

    @Autowired
    protected EtlTaskCheckpointRepository checkpointRepository;

    @Autowired
    protected OrchestratorProperties orchProps;

    @Autowired
    protected DispatchProperties dispatchProps;

    @Autowired
    protected RoutingProperties routing;

    @Autowired
    protected TokenBucket tokenBucket;

    @Autowired
    protected MeterRegistry meterRegistry;

    @Autowired
    protected ObjectMapper objectMapper;

    @Autowired
    protected KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    protected RedissonClient redisson;

    /** Feign 替身：默认返回 null（V-1 失败），需要成功链路时用 {@link #stubDataSource()}。 */
    @MockBean
    protected DataSourceServiceClient dataSourceServiceClient;

    private final List<String> seededTaskIds = new ArrayList<>();
    private final List<Long> seededDataSourceIds = new ArrayList<>();
    private final List<RLock> heldLocks = new ArrayList<>();
    private Duration[] originalTimeouts;
    private int originalMaxAttempts;

    @BeforeEach
    void rememberProperties() {
        originalTimeouts = new Duration[]{orchProps.getDispatchHeartbeatTimeout(),
                orchProps.getCancelAckTimeout(), orchProps.getScheduledStarvationTimeout(),
                orchProps.getThrottledStarvationTimeout(), orchProps.getValidatedTimeout(),
                orchProps.getCreatedTimeout(), orchProps.getStallFallbackThreshold()};
        originalMaxAttempts = orchProps.getMaxAttempts();
    }

    @AfterEach
    void restoreAndClean() {
        // 0) 先释放测试持有的分布式锁，再复原属性
        for (RLock lock : heldLocks) {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
        heldLocks.clear();

        // 1) 属性复原：上下文被多个 IT 类共享，泄漏的阈值/开关会污染后面的用例
        orchProps.setDispatchHeartbeatTimeout(originalTimeouts[0]);
        orchProps.setCancelAckTimeout(originalTimeouts[1]);
        orchProps.setScheduledStarvationTimeout(originalTimeouts[2]);
        orchProps.setThrottledStarvationTimeout(originalTimeouts[3]);
        orchProps.setValidatedTimeout(originalTimeouts[4]);
        orchProps.setCreatedTimeout(originalTimeouts[5]);
        orchProps.setStallFallbackThreshold(originalTimeouts[6]);
        orchProps.setMaxAttempts(originalMaxAttempts);
        routing.setNewWorker(false);

        // 2) 数据复原：etl_task 删除级联到 run 与 checkpoint
        for (String taskId : seededTaskIds) {
            jdbc.update("DELETE FROM etl_task WHERE task_id = ?", taskId);
        }
        seededTaskIds.clear();
        for (Long dataSourceId : seededDataSourceIds) {
            jdbc.update("DELETE FROM data_source WHERE id = ?", dataSourceId);
        }
        seededDataSourceIds.clear();
    }

    /**
     * 占住派工泵的 Redisson 锁。打开 {@code newWorker} 的用例几乎都需要它：否则 5s 周期的
     * {@code DispatchService.pump()} 会与用例的断言赛跑（同线程可重入，所以用例自己调 pump 不受影响）。
     */
    protected void holdDispatchPumpLock() {
        RLock lock = redisson.getLock(DISPATCH_PUMP_LOCK);
        lock.lock();
        heldLocks.add(lock);
    }

    // ------------------------------------------------------------------
    // 夹具：数据源 / 任务 / run / checkpoint
    // ------------------------------------------------------------------

    /** 往 data_source 插一行（IT 源库定位），返回自增 id。 */
    protected long seedDataSource() {
        String name = DATASOURCE_NAME_PREFIX + UUID.randomUUID();
        Long id = jdbc.queryForObject("INSERT INTO data_source (name, type, url, username, password, driver_class_name) "
                        + "VALUES (?, ?, ?, ?, ?, ?) RETURNING id",
                Long.class, name, ItInfra.SOURCE_TYPE, ItInfra.SOURCE_URL,
                ItInfra.PG_USER, ItInfra.PG_PASSWORD, ItInfra.SOURCE_DRIVER);
        seededDataSourceIds.add(id);
        return id;
    }

    /** 登记需要在用例结束后清理的 taskId（走真入口新建的 task 也须登记）。 */
    protected void trackTask(String taskId) {
        seededTaskIds.add(taskId);
    }

    /** 插一行 etl_task（含 data_source 外键），返回 taskId；用例结束自动清理。 */
    protected String seedTask(String targetTable) {
        return seedTask(ItInfra.SOURCE_TABLE, targetTable);
    }

    /** 插一行 etl_task（含 data_source 外键），返回 taskId；用例结束自动清理。 */
    protected String seedTask(String sourceTable, String targetTable) {
        long dataSourceId = seedDataSource();
        String taskId = "it-" + UUID.randomUUID();
        jdbc.update("INSERT INTO etl_task (task_id, data_source_id, source_table_name, target_table_name, "
                        + "status, total_shards, completed_shards) VALUES (?, ?, ?, ?, 'CREATED', 0, 0)",
                taskId, dataSourceId, sourceTable, targetTable);
        seededTaskIds.add(taskId);
        return taskId;
    }

    /**
     * 一次造一个“task + 其唯一当前 run”的夹具。
     *
     * <p>必须一个 task 只配一个非终态当前 run：{@code ux_run_task_current}（INV-1）正是这么要求的，
     * 同一 task 上种多个 current run 是夹具错而不是被测行为错。
     */
    protected RunFixture seedRunFixture(EtlTaskStatus status, int attemptNo, int shardTotal) {
        String taskId = seedTask("it_target_" + UUID.randomUUID().toString().substring(0, 8));
        return new RunFixture(taskId, seedRun(taskId, status, attemptNo, shardTotal));
    }

    /** task 与 run 的成对身份（guard/sweeper 用例的基本输入）。 */
    protected record RunFixture(String taskId, String runId) {
    }

    /** 直插一条指定状态的 run（测试夹具；生产写路径仍是 transition()）。 */
    protected String seedRun(String taskId, EtlTaskStatus status) {
        return seedRun(taskId, status, 1, 0);
    }

    protected String seedRun(String taskId, EtlTaskStatus status, int attemptNo, int shardTotal) {
        String runId = "it-run-" + UUID.randomUUID();
        jdbc.update("INSERT INTO etl_task_run (run_id, task_id, attempt_no, status, is_current, shard_total) "
                        + "VALUES (?, ?, ?, ?, TRUE, ?)",
                runId, taskId, attemptNo, status.name(), shardTotal);
        return runId;
    }

    /** 造一个已派工（DISPATCHED）且派工基线在 {@code age} 之前的 run，用于心跳超时判定。 */
    protected String seedDispatchedRun(String taskId, Duration dispatchedAgo, int attemptNo) {
        String runId = seedRun(taskId, EtlTaskStatus.DISPATCHED, attemptNo, 1);
        jdbc.update("UPDATE etl_task_run SET dispatched_at = now() - (?)::interval WHERE run_id = ?",
                seconds(dispatchedAgo), runId);
        return runId;
    }

    protected void seedCheckpoint(String taskId, String runId, String shardId, EtlShardStatus status,
                                  long rowsIn, long rowsOut, long rowsDropped) {
        seedCheckpoint(taskId, runId, shardId, status, rowsIn, rowsOut, rowsDropped, 0L, null);
    }

    protected void seedCheckpoint(String taskId, String runId, String shardId, EtlShardStatus status,
                                  long rowsIn, long rowsOut, long rowsDropped, long rowsDeduped,
                                  String errorClass) {
        jdbc.update("INSERT INTO etl_task_checkpoint "
                        + "(task_id, run_id, shard_id, status, rows_in, rows_out, rows_dropped, rows_deduped, "
                        + " error_class, pk_from, pk_to, updated_at) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, now())",
                taskId, runId, shardId, status.name(), rowsIn, rowsOut, rowsDropped, rowsDeduped,
                errorClass, rowsIn + rowsDropped);
    }

    /** 把 checkpoint 的 updated_at 往前推（卡死/活动判定用）。 */
    protected void backdateCheckpoint(String runId, String shardId, Duration age) {
        jdbc.update("UPDATE etl_task_checkpoint SET updated_at = now() - (?)::interval "
                + "WHERE run_id = ? AND shard_id = ?", seconds(age), runId, shardId);
    }

    protected void backdateRunUpdatedAt(String runId, Duration age) {
        jdbc.update("UPDATE etl_task_run SET updated_at = now() - (?)::interval WHERE run_id = ?",
                seconds(age), runId);
    }

    protected void setRunLastProgress(String runId, Duration age) {
        jdbc.update("UPDATE etl_task_run SET last_progress_ts = now() - (?)::interval WHERE run_id = ?",
                seconds(age), runId);
    }

    protected void setRunCancelRequested(String runId, Duration age) {
        jdbc.update("UPDATE etl_task_run SET cancel_requested_at = now() - (?)::interval WHERE run_id = ?",
                seconds(age), runId);
    }

    protected void setRunNextAttemptAt(String runId, Date when) {
        jdbc.update("UPDATE etl_task_run SET next_attempt_at = ? WHERE run_id = ?", when, runId);
    }

    /** 让 {@link DataSourceServiceClient} 返回一个真可连的 PG 数据源（IT 源表所在库）。 */
    protected long stubDataSource() {
        long dataSourceId = seedDataSource();
        com.lawyus.dataetl.taskmanagement.client.DataSource ds =
                new com.lawyus.dataetl.taskmanagement.client.DataSource();
        ds.setId(dataSourceId);
        ds.setName(DATASOURCE_NAME_PREFIX + dataSourceId);
        ds.setType(ItInfra.SOURCE_TYPE);
        ds.setUrl(ItInfra.SOURCE_URL);
        ds.setUsername(ItInfra.PG_USER);
        ds.setPassword(ItInfra.PG_PASSWORD);
        ds.setDriverClassName(ItInfra.SOURCE_DRIVER);
        org.mockito.BDDMockito.given(dataSourceServiceClient.getDataSourceById(dataSourceId)).willReturn(ds);
        return dataSourceId;
    }

    // ------------------------------------------------------------------
    // 读侧断言助手
    // ------------------------------------------------------------------

    protected EtlTaskStatus runStatus(String runId) {
        return EtlTaskStatus.valueOf(jdbc.queryForObject(
                "SELECT status FROM etl_task_run WHERE run_id = ?", String.class, runId));
    }

    protected String taskStatus(String taskId) {
        return jdbc.queryForObject("SELECT status FROM etl_task WHERE task_id = ?", String.class, taskId);
    }

    protected int attemptNo(String runId) {
        return jdbc.queryForObject("SELECT attempt_no FROM etl_task_run WHERE run_id = ?", Integer.class, runId);
    }

    protected String runColumn(String runId, String column) {
        return jdbc.queryForObject("SELECT " + column + " FROM etl_task_run WHERE run_id = ?", String.class, runId);
    }

    protected Date runDateColumn(String runId, String column) {
        return jdbc.queryForObject("SELECT " + column + " FROM etl_task_run WHERE run_id = ?", Date.class, runId);
    }

    /**
     * run 行的数值列（计数投影断言用）：{@code shard_*} 与 {@code rows_*} 统一按 long 读回，
     * 便于与 {@link com.lawyus.dataetl.taskmanagement.orchestrator.RunAggregate} 直接对齐。
     */
    protected long runLongColumn(String runId, String column) {
        Long value = jdbc.queryForObject("SELECT " + column + " FROM etl_task_run WHERE run_id = ?",
                Long.class, runId);
        return value == null ? 0L : value;
    }

    protected String shardStatus(String runId, String shardId) {
        return jdbc.queryForObject("SELECT status FROM etl_task_checkpoint WHERE run_id = ? AND shard_id = ?",
                String.class, runId, shardId);
    }

    /** 某 run 下各分片状态（shard_id → status），便于断言多分片形态。 */
    protected java.util.Map<String, String> shardStatuses(String runId) {
        java.util.Map<String, String> statuses = new java.util.LinkedHashMap<>();
        for (Map<String, Object> row : jdbc.queryForList(
                "SELECT shard_id, status FROM etl_task_checkpoint WHERE run_id = ? ORDER BY shard_id", runId)) {
            statuses.put((String) row.get("shard_id"), (String) row.get("status"));
        }
        return statuses;
    }

    /** 某 run 的 {@code shard_id:pk_from-pk_to} 升序列表（验证分片计划与 §7.2 持久化区间）。 */
    protected List<String> shardRanges(String runId) {
        List<String> ranges = new ArrayList<>();
        for (Map<String, Object> row : jdbc.queryForList("SELECT shard_id, pk_from, pk_to "
                + "FROM etl_task_checkpoint WHERE run_id = ? ORDER BY shard_id", runId)) {
            ranges.add(row.get("shard_id") + ":" + row.get("pk_from") + "-" + row.get("pk_to"));
        }
        return ranges;
    }

    /** 计数指标当前值（无标签或标签成对：{@code name, "from", "RUNNING"}）。 */
    protected double counter(String name, String... tags) {
        var sample = meterRegistry.find(name).tags(tags).counter();
        return sample == null ? 0d : sample.count();
    }

    /** {@code etl.guard.failure.total{guard=...}} 便捷读数。 */
    protected double guardFailure(String guard) {
        return counter("etl.guard.failure.total", "guard", guard);
    }

    protected TransitionContext.Builder ctx(String taskId) {
        return TransitionContext.builder(taskId).actor("it");
    }

    private static String seconds(Duration duration) {
        return duration.toSeconds() + " seconds";
    }
}
