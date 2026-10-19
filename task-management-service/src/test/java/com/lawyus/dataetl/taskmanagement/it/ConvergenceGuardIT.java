package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlShardStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.ErrorClass;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.RunAggregate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;

import java.util.Date;

import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.CANCELLED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.DISPATCHED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.FAILED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.NEEDS_ATTENTION;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.PARTIAL_SUCCESS;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.QUEUED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RETRYING;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RUNNING;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.SUCCEEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * §12.1 矩阵第 3/4/5 行 + G-4/G-5：对账、dedup 跨 attempt、quiesce 在真库上的表现。
 *
 * <p>分片终态由"worker"（这里用 SQL 夹具）直写 {@code etl_task_checkpoint}，orchestrator 侧只做聚合重算——
 * 正是 §4.1 写权矩阵与 §7.5"results 只是触发器"的口径。
 */
class ConvergenceGuardIT extends AbstractOrchestratorIT {

    @Test
    @DisplayName("G-4 对账：rows_out+rows_dropped ≠ rows_in 时不得进 SUCCEEDED，必走 T13")
    void rowImbalanceBlocksSuccessAndRoutesNeedsAttention() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_g4");
        String runId = seedRun(taskId, RUNNING, 1, 1);
        // 少报 40 行：等式不成立（DB 行级 CHECK 只禁止"多写"，§4.2 INV-4）
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.SUCCEEDED, 100, 60, 0);

        RunAggregate aggregate = checkpointRepository.aggregate(runId);
        assertThat(aggregate.isBalanced()).isFalse();
        assertThat(aggregate.getSucceeded()).isEqualTo(1L);

        double before = guardFailure("G-4_BALANCE");
        assertThatThrownBy(() -> orchestrator.transition(runId, RUNNING, SUCCEEDED,
                ctx(taskId).reason("IT_G4").maxAttempts(orchProps.getMaxAttempts()).build()))
                .isInstanceOf(GuardFailureException.class)
                .hasMessageContaining("rows_out + rows_dropped != rows_in");
        assertThat(guardFailure("G-4_BALANCE") - before).isEqualTo(1.0);
        assertThat(runStatus(runId)).isEqualTo(RUNNING);

        // 终局由聚合决定：进 NEEDS_ATTENTION（T13），并落 error_summary / finished_at
        orchestrator.transition(runId, RUNNING, NEEDS_ATTENTION,
                ctx(taskId).reason("ROW_BALANCE_VIOLATION").errorClass(ErrorClass.FATAL).build());
        assertThat(runStatus(runId)).isEqualTo(NEEDS_ATTENTION);
        assertThat(runColumn(runId, "error_summary")).contains("ROW_BALANCE_VIOLATION");
        assertThat(runDateColumn(runId, "finished_at"))
                .as("NEEDS_ATTENTION 非终态：不得写 finished_at（INV-10 的前提）").isNull();
        assertThat(taskStatus(taskId)).isEqualTo("NEEDS_ATTENTION");
        // 计数快照同时落库，且原样反映不平——投影不得粉饰太平地把越量遮住
        assertThat(runLongColumn(runId, "rows_in")).isEqualTo(100L);
        assertThat(runLongColumn(runId, "rows_out")).isEqualTo(60L);
        assertThat(runLongColumn(runId, "shard_succeeded")).isEqualTo(1L);
    }

    @Test
    @DisplayName("dedup 跨 attempt：第二次全 ON CONFLICT 命中，等式仍平，能进 SUCCEEDED（D-08）")
    void dedupAcrossAttemptKeepsBalanceAndReachesSuccess() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_dedup");
        String runId = seedRun(taskId, RUNNING, 1, 1);
        // attempt 1 写 100 行；attempt 2 重放全部 ON CONFLICT DO NOTHING → 只有 deduped 增长
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.SUCCEEDED, 100, 100, 0, 100L, null);

        RunAggregate aggregate = checkpointRepository.aggregate(runId);
        assertThat(aggregate.isBalanced()).as("rows_deduped 不入对账等式").isTrue();

        orchestrator.transition(runId, RUNNING, SUCCEEDED,
                ctx(taskId).reason("ALL_SHARDS_OK").maxAttempts(orchProps.getMaxAttempts()).build());
        assertThat(runStatus(runId)).isEqualTo(SUCCEEDED);
        assertThat(taskStatus(taskId)).isEqualTo("SUCCEEDED");
        assertThat(runDateColumn(runId, "finished_at")).isNotNull();
        // rows_deduped 不入对账等式，但它是 run 行的一个真实列：不投影则它永远停在 0
        assertThat(runLongColumn(runId, "rows_deduped")).as("幂等命中数也投影").isEqualTo(100L);
        assertThat(runLongColumn(runId, "rows_in")).isEqualTo(100L);
        assertThat(runLongColumn(runId, "rows_out")).isEqualTo(100L);
    }

    @Test
    @DisplayName("INV-4 源头兜底：单分片 rows_out + rows_dropped > rows_in 被 DB CHECK 拒绝")
    void rowLevelCheckRejectsOverwrite() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_check");
        String runId = seedRun(taskId, RUNNING, 1, 1);

        assertThatThrownBy(() -> seedCheckpoint(taskId, runId, "shard-0",
                EtlShardStatus.SUCCEEDED, 10, 11, 0))
                .isInstanceOf(DataAccessException.class)
                .hasMessageContaining("ck_checkpoint_rows");
        assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM etl_task_checkpoint WHERE run_id = ?",
                Integer.class, runId)).isZero();
    }

    @Test
    @DisplayName("quiesce：一个分片在跑 + 一个 FAILED(PERSISTENT) 时不许进 RETRYING；收敛进 PARTIAL_SUCCESS")
    void quiesceBlocksRetryingAndConvergesToPartialSuccess() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_quiesce");
        String runId = seedRun(taskId, RUNNING, 1, 2);
        seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.RUNNING, 10, 5, 0);
        seedCheckpoint(taskId, runId, "shard-1", EtlShardStatus.FAILED, 10, 8, 2, 0L, "PERSISTENT");

        assertThat(checkpointRepository.countActiveShards(runId)).isEqualTo(1L);
        double before = guardFailure("G-3_QUIESCE");
        assertThatThrownBy(() -> orchestrator.transition(runId, RUNNING, RETRYING,
                ctx(taskId).reason("IT_QUIESCE").maxAttempts(orchProps.getMaxAttempts()).build()))
                .isInstanceOf(GuardFailureException.class)
                .hasMessageContaining("仍有 ASSIGNED/RUNNING 分片");
        assertThat(guardFailure("G-3_QUIESCE") - before).isEqualTo(1.0);
        assertThat(runStatus(runId)).isEqualTo(RUNNING);

        // 在跑的分片收尾 → 一成一败 → PARTIAL_SUCCESS（T11，G-2 要求存在 FAILED 分片）
        jdbc.update("UPDATE etl_task_checkpoint SET status = 'SUCCEEDED', rows_out = 10, rows_dropped = 0 "
                + "WHERE run_id = ? AND shard_id = 'shard-0'", runId);
        orchestrator.transition(runId, RUNNING, PARTIAL_SUCCESS,
                ctx(taskId).reason("SOME_SHARDS_FAILED").build());
        assertThat(runStatus(runId)).isEqualTo(PARTIAL_SUCCESS);

        // 无 FAILED 分片时不许进 PARTIAL_SUCCESS（G-2 前置），否则"宽恕"后无法收敛
        // 注：一个 task 只能有一个非终态当前 run（INV-1），第二个 run 必须配自己的 task
        RunFixture other = seedRunFixture(RUNNING, 1, 1);
        seedCheckpoint(other.taskId(), other.runId(), "shard-0", EtlShardStatus.SUCCEEDED, 10, 10, 0);
        assertThatThrownBy(() -> orchestrator.transition(other.runId(), RUNNING, PARTIAL_SUCCESS,
                ctx(other.taskId()).reason("IT_G2").build()))
                .isInstanceOf(GuardFailureException.class)
                .hasMessageContaining("G-2_PARTIAL");
    }

    @Test
    @DisplayName("G-5 attempt 预算：attempt 耗尽禁自动重试（T8/T12），但人工出边 T22 仍可达 NEEDS_ATTENTION")
    void attemptBudgetBlocksAutoRetryButNotHumanExit() {
        int exhausted = orchProps.getMaxAttempts();

        RunFixture dispatched = seedRunFixture(DISPATCHED, exhausted, 0);
        assertThatThrownBy(() -> orchestrator.transition(dispatched.runId(), DISPATCHED, QUEUED,
                ctx(dispatched.taskId()).reason("IT_G5").maxAttempts(exhausted).build()))
                .isInstanceOf(GuardFailureException.class)
                .hasMessageContaining("G-5_ATTEMPTS");

        RunFixture running = seedRunFixture(RUNNING, exhausted, 0);
        assertThatThrownBy(() -> orchestrator.transition(running.runId(), RUNNING, RETRYING,
                ctx(running.taskId()).reason("IT_G5").maxAttempts(exhausted).build()))
                .isInstanceOf(GuardFailureException.class)
                .hasMessageContaining("G-5_ATTEMPTS");

        // T22：attempt 耗尽后的正确归宿是"人来处理"，不是无限回退
        RunFixture retrying = seedRunFixture(RETRYING, exhausted, 0);
        orchestrator.transition(retrying.runId(), RETRYING, NEEDS_ATTENTION,
                ctx(retrying.taskId()).reason("ATTEMPTS_EXHAUSTED").errorClass(ErrorClass.PERSISTENT).build());
        assertThat(runStatus(retrying.runId())).isEqualTo(NEEDS_ATTENTION);
        assertThat(attemptNo(retrying.runId())).isEqualTo(exhausted);
        assertThat(taskStatus(retrying.taskId())).isEqualTo("NEEDS_ATTENTION");
    }

    @Test
    @DisplayName("T24/T25 人工出口：NEEDS_ATTENTION 可回 QUEUED 重排、可回 RUNNING 续跑（不改 attempt）")
    void humanExitsFromNeedsAttention() {
        RunFixture retry = seedRunFixture(NEEDS_ATTENTION, 2, 1);
        seedCheckpoint(retry.taskId(), retry.runId(), "shard-0", EtlShardStatus.FAILED, 10, 0, 0, 0L, "PERSISTENT");

        orchestrator.transition(retry.runId(), NEEDS_ATTENTION, QUEUED,
                ctx(retry.taskId()).reason("HUMAN_RETRY").build());
        assertThat(runStatus(retry.runId())).isEqualTo(QUEUED);
        assertThat(attemptNo(retry.runId())).isEqualTo(2);   // 人工重排不自增 attempt
        assertThat(runColumn(retry.runId(), "queued_reason")).isEqualTo("SCHEDULED");

        RunFixture resume = seedRunFixture(NEEDS_ATTENTION, 2, 1);
        orchestrator.transition(resume.runId(), NEEDS_ATTENTION, RUNNING,
                ctx(resume.taskId()).reason("HUMAN_RESUME").build());
        assertThat(runStatus(resume.runId())).isEqualTo(RUNNING);
        assertThat(attemptNo(resume.runId())).isEqualTo(2);

        RunFixture abort = seedRunFixture(NEEDS_ATTENTION, 2, 1);
        orchestrator.transition(abort.runId(), NEEDS_ATTENTION, FAILED,
                ctx(abort.taskId()).reason("HUMAN_ABORT").errorClass(ErrorClass.FATAL).build());
        assertThat(runStatus(abort.runId())).isEqualTo(FAILED);
        assertThat(runDateColumn(abort.runId(), "finished_at")).isNotNull();

        // 终态后唯一出边：SUPERSEDED；取消类边一律非法（禁止清单 #1）
        assertThatThrownBy(() -> orchestrator.transition(abort.runId(), FAILED, CANCELLED,
                ctx(abort.taskId()).reason("IT_TERMINAL").build()))
                .isInstanceOf(IllegalStateTransitionException.class);
    }
}
