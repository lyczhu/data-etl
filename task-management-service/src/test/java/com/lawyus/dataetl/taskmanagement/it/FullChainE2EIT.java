package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.RunAggregate;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.DispatchService;
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import com.lawyus.dataetl.worker.WorkerApplication;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;

import java.time.Duration;
import java.util.List;

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
        workerContext = new SpringApplicationBuilder(WorkerApplication.class)
                .web(WebApplicationType.NONE)
                .run("--spring.config.location=classpath:/it-worker.yml",
                        "--spring.cloud.config.enabled=false",
                        "--spring.datasource.url=" + ItInfra.IT_DB_URL,
                        "--spring.kafka.bootstrap-servers=" + ItInfra.KAFKA_BOOTSTRAP,
                        "--spring.kafka.consumer.auto-offset-reset=latest",
                        "--etl.topics.dispatch-batch=" + ItInfra.TOPIC_DISPATCH,
                        "--etl.topics.progress=" + ItInfra.TOPIC_PROGRESS,
                        "--etl.topics.results=" + ItInfra.TOPIC_RESULTS,
                        "--etl.topics.dlt=" + ItInfra.TOPIC_DLT);
    }

    @AfterAll
    static void stopRealWorker() {
        if (workerContext != null) {
            workerContext.close();
            workerContext = null;
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
                        + "SELECT run_id, SUM(rows_in) rows_in, SUM(rows_out) rows_out, SUM(rows_dropped) rows_dropped "
                        + "FROM etl_task_checkpoint GROUP BY run_id) a ON a.run_id = ru.run_id "
                        + "WHERE ru.status = 'SUCCEEDED' AND ru.rows_out + ru.rows_dropped <> ru.rows_in"))
                .as("校验 2（INV-4）：SUCCEEDED 的 run 行数必平").isEmpty();
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
