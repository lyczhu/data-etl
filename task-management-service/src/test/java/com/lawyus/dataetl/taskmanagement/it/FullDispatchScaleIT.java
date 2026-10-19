package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.dispatch.DispatchService;
import com.lawyus.dataetl.taskmanagement.service.EtlTaskService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Gate 1 · #1 全量派工（方案 v2 WP-01 / 评审 §9.1 #1）。
 *
 * <p>用一个 120 分片的真实任务验证：
 * <ul>
 *   <li>分片数 = {@code shard-target-count}（并行单元），不再等于 chunk 数；</li>
 *   <li>{@code pump()} 对仍有 PENDING 的 run 持续补派，而非只派首批 {@code permitsPerRun=8} 片；</li>
 *   <li>全部 PENDING 最终变为 ASSIGNED，结束后 {@code COUNT(PENDING)=0}。</li>
 * </ul>
 *
 * <p>这里不启动 worker：本用例只验证“派工侧把计划全部发出去”。真实执行与收敛由
 * {@link FullChainE2EIT} 的 {@code crashAfterTargetCommitReplaysIdempotently} /
 * {@code duplicateDispatchAndResultsAreIdempotent} 与 {@link TwoWorkerConcurrencyIT} 覆盖。
 * WP-12 后全局许可真源是 DB 活跃分片数，因此本用例把 {@code max-concurrent-shards}
 * 临时调到 120，验证“容量足够时一次 pump 全部派完”。
 */
class FullDispatchScaleIT extends AbstractOrchestratorIT {

    private static final String SCALE_SOURCE_TABLE = "it_source_scale";
    private static final int SCALE_ROWS = 120;

    @Autowired
    private EtlTaskService etlTaskService;

    @Autowired
    private DispatchService dispatchService;

    private int originalPermitsPerRun;
    private int originalShardTargetCount;
    private int originalMaxConcurrentShards;

    @BeforeAll
    static void createItTopics() {
        ItInfra.ensureItTopics();
    }

    @BeforeEach
    void retainPumpAndProperties() {
        holdDispatchPumpLock();
        originalPermitsPerRun = dispatchProps.getPermitsPerRun();
        originalShardTargetCount = dispatchProps.getShardTargetCount();
        originalMaxConcurrentShards = dispatchProps.getMaxConcurrentShards();
        seedScaleSource();
    }

    @AfterEach
    void restoreScaleFixtures() {
        dispatchProps.setPermitsPerRun(originalPermitsPerRun);
        dispatchProps.setShardTargetCount(originalShardTargetCount);
        dispatchProps.setMaxConcurrentShards(originalMaxConcurrentShards);
        jdbc.execute("DROP TABLE IF EXISTS " + SCALE_SOURCE_TABLE);
    }

    @Test
    @DisplayName("120 分片：一次 pump 在令牌充足时补齐全部 PENDING，结束后 COUNT(PENDING)=0")
    void pumpDispatchesEveryPendingShard() {
        routing.setNewWorker(true);
        dispatchProps.setShardTargetCount(SCALE_ROWS);
        dispatchProps.setPermitsPerRun(SCALE_ROWS);
        dispatchProps.setMaxConcurrentShards(SCALE_ROWS);   // WP-12：DB 全局许可容量

        long dataSourceId = stubDataSource();
        EtlTask request = new EtlTask();
        request.setDataSourceId(dataSourceId);
        request.setSourceTableName(SCALE_SOURCE_TABLE);
        request.setDescription("Gate1 #1 full dispatch 120 shards");
        EtlTask created = etlTaskService.createTask(request);
        String taskId = created.getTaskId();
        trackTask(taskId);

        String runId = currentRunId(taskId);
        assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.QUEUED);
        assertThat(shardStatuses(runId)).hasSize(SCALE_ROWS);
        assertThat(countPending(runId)).isEqualTo(SCALE_ROWS);

        dispatchService.pump();

        assertThat(runStatus(runId)).as("首片派出触发 T5（QUEUED→DISPATCHED），且只触发一次")
                .isEqualTo(EtlTaskStatus.DISPATCHED);
        Map<String, String> statuses = shardStatuses(runId);
        assertThat(statuses).hasSize(SCALE_ROWS);
        assertThat(statuses.values()).as("所有分片都必须离开 PENDING").containsOnly("ASSIGNED");
        assertThat(countPending(runId)).as("Gate1 #1：COUNT(PENDING)=0").isZero();
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM etl_task_checkpoint "
                        + "WHERE run_id = ? AND status = 'ASSIGNED'", Integer.class, runId))
                .isEqualTo(SCALE_ROWS);
    }

    private long countPending(String runId) {
        return checkpointRepository.countPendingShards(runId);
    }

    private String currentRunId(String taskId) {
        return jdbc.queryForObject("SELECT run_id FROM etl_task_run WHERE task_id = ? AND is_current",
                String.class, taskId);
    }

    /** 建一张 120 行、整型主键的源表；planShards 会真连它并生成 120 个区间。 */
    private void seedScaleSource() {
        jdbc.execute("DROP TABLE IF EXISTS " + SCALE_SOURCE_TABLE);
        jdbc.execute("CREATE TABLE " + SCALE_SOURCE_TABLE + " ("
                + "id BIGINT PRIMARY KEY, name VARCHAR(64), amount NUMERIC(10, 2), "
                + "note VARCHAR(120), created_at TIMESTAMP)");
        for (int i = 1; i <= SCALE_ROWS; i++) {
            jdbc.update("INSERT INTO " + SCALE_SOURCE_TABLE
                            + " (id, name, amount, note, created_at) VALUES (?, ?, ?, ?, now())",
                    (long) i, "scale-" + i, 10.50, "scale fixture " + i);
        }
    }
}
