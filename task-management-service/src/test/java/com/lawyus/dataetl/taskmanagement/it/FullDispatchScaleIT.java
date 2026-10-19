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
 * 令牌用 {@code release} 模拟 worker 完成后的归还，否则全局桶（24）会按背压语义把派工停在 24 片——
 * 那正是 THROTTLED 的正确行为，不是本用例要证伪的对象。
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
    private int originalAvailablePermits;

    @BeforeAll
    static void createItTopics() {
        ItInfra.ensureItTopics();
    }

    @BeforeEach
    void retainPumpAndProperties() {
        holdDispatchPumpLock();
        originalPermitsPerRun = dispatchProps.getPermitsPerRun();
        originalShardTargetCount = dispatchProps.getShardTargetCount();
        originalAvailablePermits = tokenBucket.available();
        seedScaleSource();
    }

    @AfterEach
    void restoreScaleFixtures() {
        dispatchProps.setPermitsPerRun(originalPermitsPerRun);
        dispatchProps.setShardTargetCount(originalShardTargetCount);
        // 把用例多释放的令牌收回，避免 available 超过容量污染其它 IT 的背压断言
        int surplus = tokenBucket.available() - originalAvailablePermits;
        if (surplus > 0) {
            tokenBucket.tryAcquireUpTo(surplus);
        }
        jdbc.execute("DROP TABLE IF EXISTS " + SCALE_SOURCE_TABLE);
    }

    @Test
    @DisplayName("120 分片：一次 pump 在令牌充足时补齐全部 PENDING，结束后 COUNT(PENDING)=0")
    void pumpDispatchesEveryPendingShard() {
        routing.setNewWorker(true);
        dispatchProps.setShardTargetCount(SCALE_ROWS);
        dispatchProps.setPermitsPerRun(SCALE_ROWS);
        tokenBucket.release(SCALE_ROWS + 16);   // 模拟 worker 持续归还令牌

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
