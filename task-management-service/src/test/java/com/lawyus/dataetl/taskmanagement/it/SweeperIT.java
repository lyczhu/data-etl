package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlShardStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.scheduler.AcceptanceSweeper;
import com.lawyus.dataetl.taskmanagement.orchestrator.scheduler.BackoffReleaseSweeper;
import com.lawyus.dataetl.taskmanagement.orchestrator.scheduler.ConvergenceSweeper;
import com.lawyus.dataetl.taskmanagement.orchestrator.scheduler.DispatchTimeoutSweeper;
import com.lawyus.dataetl.taskmanagement.orchestrator.scheduler.StallAndCancelSweeper;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;

import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.CANCELLED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.DISPATCHED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.FAILED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.NEEDS_ATTENTION;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.QUEUED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RETRYING;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RUNNING;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * §12.1 矩阵第 7/8/10 行 + §6.5/§7.9：四个 sweeper 在真库 + 真 Redis 锁上的判定与兜底。
 *
 * <p>用例把对应阈值临时改小（{@code @AfterEach} 复原）并<b>直接调用</b> {@code sweep()}，
 * 从而把"时间推进"变成可控输入；不依赖等待真实调度周期，避免 flaky。
 */
class SweeperIT extends AbstractOrchestratorIT {

    @Autowired
    private DispatchTimeoutSweeper dispatchTimeoutSweeper;

    @Autowired
    private StallAndCancelSweeper stallAndCancelSweeper;

    @Autowired
    private BackoffReleaseSweeper backoffReleaseSweeper;

    @Autowired
    private AcceptanceSweeper acceptanceSweeper;

    @Autowired
    private ConvergenceSweeper convergenceSweeper;

    @Test
    @DisplayName("心跳丢失但 worker 仍在跑：不回退 QUEUED，而是 CAS 补登记 RUNNING（§7.9）")
    void heartbeatLostButShardActive_recoversRunning() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_recover");
        String runId = seedDispatchedRun(taskId, Duration.ofSeconds(30), 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.RUNNING, 10, 5, 0);

        orchProps.setDispatchHeartbeatTimeout(Duration.ofSeconds(10));
        dispatchTimeoutSweeper.sweep();

        assertThat(runStatus(runId)).isEqualTo(RUNNING);
        assertThat(attemptNo(runId)).as("补登记不消耗 attempt 预算").isEqualTo(1);
        assertThat(runColumn(runId, "started_at")).isNotNull();
        assertThat(runColumn(runId, "error_summary")).isNull();
    }

    @Test
    @DisplayName("DISPATCHED 超时无在跑分片：T8 回 QUEUED 且 attempt+1、排定退避")
    void dispatchTimeout_quiesced_fallsBackToQueuedWithAttemptBump() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_fallback");
        String runId = seedDispatchedRun(taskId, Duration.ofSeconds(30), 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.PENDING, 0, 0, 0);

        orchProps.setDispatchHeartbeatTimeout(Duration.ofSeconds(10));
        dispatchTimeoutSweeper.sweep();

        assertThat(runStatus(runId)).isEqualTo(QUEUED);
        assertThat(attemptNo(runId)).isEqualTo(2);
        assertThat(runDateColumn(runId, "next_attempt_at")).isNotNull();
        assertThat(taskStatus(taskId)).isEqualTo("QUEUED");
    }

    @Test
    @DisplayName("attempt 耗尽的派工超时：落 NEEDS_ATTENTION（T9），不再无限回退")
    void dispatchTimeout_attemptsExhausted_goesNeedsAttention() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_exhausted");
        String runId = seedDispatchedRun(taskId, Duration.ofSeconds(30), orchProps.getMaxAttempts());
        // 上一 attempt 已读过几行就挂了：FAILED 不是活跃分片，不影响本用例的 T9 判定
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.FAILED, 7, 3, 0);

        orchProps.setDispatchHeartbeatTimeout(Duration.ofSeconds(10));
        dispatchTimeoutSweeper.sweep();

        assertThat(runStatus(runId)).isEqualTo(NEEDS_ATTENTION);
        assertThat(runColumn(runId, "error_summary")).contains("NO_WORKER_HEARTBEAT");
        // 全程没有一条 results 消息：计数快照必须由迁移自已完成，否则 run 行停在 0
        assertThat(runLongColumn(runId, "rows_in")).as("sweeper 驱动的终局也要投影").isEqualTo(7L);
        assertThat(runLongColumn(runId, "rows_out")).isEqualTo(3L);
        assertThat(runLongColumn(runId, "shard_failed")).isEqualTo(1L);
    }

    @Test
    @DisplayName("取消不可达：超 ack 窗口且 quiesce 成立 → T16 强制 CANCELLED")
    void cancelAckTimeoutWithoutActiveShard_forcesCancelled() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_cancel");
        String runId = seedRun(taskId, RUNNING, 1, 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.PENDING, 0, 0, 0);
        setRunCancelRequested(runId, Duration.ofSeconds(5));

        orchProps.setCancelAckTimeout(Duration.ofSeconds(1));
        stallAndCancelSweeper.sweep();

        assertThat(runStatus(runId)).isEqualTo(CANCELLED);
        assertThat(runDateColumn(runId, "finished_at")).isNotNull();
        assertThat(taskStatus(taskId)).isEqualTo("CANCELLED");
    }

    @Test
    @DisplayName("取消请求超时但仍有活跃分片：不强制，等 worker 在 chunk 边界自停")
    void cancelAckTimeoutWithActiveShard_isNotForced() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_cancel_active");
        String runId = seedRun(taskId, RUNNING, 1, 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.RUNNING, 10, 5, 0);
        setRunCancelRequested(runId, Duration.ofSeconds(5));

        orchProps.setCancelAckTimeout(Duration.ofSeconds(1));
        stallAndCancelSweeper.sweep();

        assertThat(runStatus(runId)).isEqualTo(RUNNING);
        assertThat(runDateColumn(runId, "finished_at")).isNull();
    }

    @Test
    @DisplayName("RUNNING 卡死（心跳与 checkpoint 皆静默）→ T13 NEEDS_ATTENTION(STALLED)")
    void stalledRun_movesToNeedsAttention() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_stall");
        String runId = seedRun(taskId, RUNNING, 1, 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.ASSIGNED, 0, 0, 0);
        setRunLastProgress(runId, Duration.ofSeconds(60));
        backdateCheckpoint(runId, "shard-0", Duration.ofSeconds(60));
        jdbc.update("UPDATE etl_task_run SET started_at = now() - interval '120 seconds' WHERE run_id = ?", runId);

        orchProps.setStallFallbackThreshold(Duration.ofSeconds(30));
        stallAndCancelSweeper.sweep();

        assertThat(runStatus(runId)).isEqualTo(NEEDS_ATTENTION);
        assertThat(runColumn(runId, "error_summary")).contains("STALLED");
    }

    @Test
    @DisplayName("RUNNING 但心跳新鲜：不判卡死（阈值命中前先叠活动样本）")
    void freshHeartbeat_isNotStalled() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_not_stall");
        String runId = seedRun(taskId, RUNNING, 1, 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.RUNNING, 10, 5, 0);
        setRunLastProgress(runId, Duration.ofSeconds(2));

        orchProps.setStallFallbackThreshold(Duration.ofSeconds(30));
        stallAndCancelSweeper.sweep();

        assertThat(runStatus(runId)).isEqualTo(RUNNING);
    }

    @Test
    @DisplayName("时间推进：T12 落库的 next_attempt_at 恰为 t0+5m（退避第一档，代码不写死）")
    void backoffSchedule_isExactFromConfiguredTiers() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_backoff");
        String runId = seedRun(taskId, RUNNING, 1, 0);
        Date t0 = Date.from(Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS));

        orchestrator.transition(runId, RUNNING, RETRYING,
                ctx(taskId).reason("IT_BACKOFF").now(t0).maxAttempts(orchProps.getMaxAttempts()).build());

        assertThat(attemptNo(runId)).isEqualTo(2);
        assertThat(runDateColumn(runId, "next_attempt_at").getTime())
                .isEqualTo(t0.getTime() + Duration.ofMinutes(5).toMillis());

        // 退避未到期不放行
        backoffReleaseSweeper.sweep();
        assertThat(runStatus(runId)).isEqualTo(RETRYING);

        // 到期后 T21 → DISPATCHED（复用同 run 的 checkpoint 续派）
        setRunNextAttemptAt(runId, new Date(System.currentTimeMillis() - 1000));
        backoffReleaseSweeper.sweep();
        assertThat(runStatus(runId)).isEqualTo(DISPATCHED);
        assertThat(runColumn(runId, "dispatched_at")).isNotNull();
    }

    @Test
    @DisplayName("T32 饥饿：SCHEDULED 与 THROTTLED 各用各自阈值，误判会过早喊人")
    void queuedStarvation_usesReasonSpecificThreshold() {
        RunFixture scheduled = seedRunFixture(QUEUED, 1, 1);
        jdbc.update("UPDATE etl_task_run SET queued_reason = 'SCHEDULED' WHERE run_id = ?", scheduled.runId());
        backdateRunUpdatedAt(scheduled.runId(), Duration.ofSeconds(30));

        RunFixture throttled = seedRunFixture(QUEUED, 1, 1);
        jdbc.update("UPDATE etl_task_run SET queued_reason = 'THROTTLED' WHERE run_id = ?", throttled.runId());
        backdateRunUpdatedAt(throttled.runId(), Duration.ofSeconds(30));

        orchProps.setScheduledStarvationTimeout(Duration.ofSeconds(10));
        orchProps.setThrottledStarvationTimeout(Duration.ofMinutes(30));
        backoffReleaseSweeper.sweep();

        assertThat(runStatus(scheduled.runId())).as("SCHEDULED 超阈值 → 喊人").isEqualTo(NEEDS_ATTENTION);
        assertThat(runColumn(scheduled.runId(), "error_summary")).contains("DISPATCH_STARVED");
        assertThat(runStatus(throttled.runId())).as("THROTTLED 阈值未到 → 继续排队").isEqualTo(QUEUED);
    }

    @Test
    @DisplayName("T30：VALIDATED 驻留超阈值 = 内部错误 FAILED(FATAL_CONFIG)")
    void validatedStuck_movesToFailed() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_validated_stuck");
        String runId = seedRun(taskId, EtlTaskStatus.VALIDATED, 1, 1);
        backdateRunUpdatedAt(runId, Duration.ofSeconds(30));

        orchProps.setValidatedTimeout(Duration.ofSeconds(5));
        acceptanceSweeper.sweep();

        assertThat(runStatus(runId)).isEqualTo(FAILED);
        assertThat(runColumn(runId, "error_summary")).contains("ENQUEUE_STALLED");
    }

    @Test
    @DisplayName("T31：CREATED 驻留（提交时校验链没跑完）→ 重放 V-1..V-4 → 分片计划落地并入队")
    void createdStuck_replaysValidationChain() {
        long dataSourceId = stubDataSource();
        String taskId = seedTaskBoundTo(ItInfra.SOURCE_TABLE, "it_target_replay", dataSourceId);
        String runId = seedRun(taskId, EtlTaskStatus.CREATED, 1, 0);
        backdateRunUpdatedAt(runId, Duration.ofSeconds(30));

        routing.setNewWorker(true);
        orchProps.setCreatedTimeout(Duration.ofSeconds(5));
        acceptanceSweeper.sweep();

        assertThat(runStatus(runId)).isEqualTo(QUEUED);
        assertThat(jdbc.queryForObject("SELECT shard_total FROM etl_task_run WHERE run_id = ?",
                Integer.class, runId)).isEqualTo(3);
        // 20 行 / chunk 8 → [1,7] [8,14] [15,20]，主键闭区间持久化（§7.2 派工只带区间不带数据）
        assertThat(shardRanges(runId)).containsExactly("shard-0:1-7", "shard-1:8-14", "shard-2:15-20");
        assertThat(shardStatuses(runId).values()).containsOnly("PENDING");
    }

    @Test
    @DisplayName("最后一条 results 丢失：sweeper 无活跃分片时重算聚合，仍能收敛 SUCCEEDED")
    void resultLostConvergesWithoutResults() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_result_lost");
        String runId = seedRun(taskId, RUNNING, 1, 1);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.SUCCEEDED, 20, 20, 0);
        routing.setNewWorker(true);

        convergenceSweeper.sweep();

        assertThat(runStatus(runId)).isEqualTo(EtlTaskStatus.SUCCEEDED);
        assertThat(runLongColumn(runId, "rows_in")).as("sweeper 驱动的终局也要投影计数").isEqualTo(20L);
        assertThat(runLongColumn(runId, "rows_out")).isEqualTo(20L);
        assertThat(taskStatus(taskId)).isEqualTo("SUCCEEDED");
    }

    @Test
    @DisplayName("T31 重放：计划已落库但 CREATED 未推进，重放删旧插新不撞唯一键")
    void createdStuckWithExistingPlan_replaysIdempotently() {
        long dataSourceId = stubDataSource();
        String taskId = seedTaskBoundTo(ItInfra.SOURCE_TABLE, "it_target_replay_existing", dataSourceId);
        String runId = seedRun(taskId, EtlTaskStatus.CREATED, 1, 3);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.PENDING, 0, 0, 0);
        seedCheckpoint(taskId, runId, "shard-1", EtlShardStatus.PENDING, 0, 0, 0);
        seedCheckpoint(taskId, runId, "shard-2", EtlShardStatus.PENDING, 0, 0, 0);
        backdateRunUpdatedAt(runId, Duration.ofSeconds(30));

        routing.setNewWorker(true);
        orchProps.setCreatedTimeout(Duration.ofSeconds(5));
        acceptanceSweeper.sweep();

        assertThat(runStatus(runId)).isEqualTo(QUEUED);
        assertThat(shardRanges(runId)).containsExactly("shard-0:1-7", "shard-1:8-14", "shard-2:15-20");
        assertThat(shardStatuses(runId).values()).containsOnly("PENDING");
    }

    /** 把 task 绑到已 stub 的数据源 id 上（{@link #seedTask} 自带一个未 stub 的数据源行）。 */
    private String seedTaskBoundTo(String sourceTable, String targetTable, long dataSourceId) {
        String taskId = seedTask(sourceTable, targetTable);
        jdbc.update("UPDATE etl_task SET data_source_id = ? WHERE task_id = ?", dataSourceId, taskId);
        return taskId;
    }
}
