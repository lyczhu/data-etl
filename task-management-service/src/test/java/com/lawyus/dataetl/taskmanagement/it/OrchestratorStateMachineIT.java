package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.taskmanagement.orchestrator.AllowedTransitions;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlShardStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.dao.DataAccessException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.CANCELLED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.DISPATCHED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.NEEDS_ATTENTION;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.PARTIAL_SUCCESS;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.QUEUED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RUNNING;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.SUCCEEDED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.SUPERSEDED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * §12.1 矩阵第 1/2/6 行 + INV-1/INV-6/INV-7：<b>真库</b>上跑状态机内核。
 *
 * <p>与单元穷举（{@code AllowedTransitionsTest}）的分工：单元测的是"迁移表本身"，本类测的是
 * "迁移表 + guard + CAS + 派生投影 + DB 约束"这套组合在真实 PostgreSQL 上的行为——尤其是
 * {@code ux_run_task_current} 部分唯一索引与 {@code ux_run_task_attempt}，它们只能由真库证明。
 */
class OrchestratorStateMachineIT extends AbstractOrchestratorIT {

    @Test
    @DisplayName("穷举 12×12=144 组合走真 transition()：恰 32 条合法，其余非法且库不变")
    void allOneHundredFortyFourCombinations_exactlyThirtyTwoLegal() {
        int legalExecuted = 0;
        int illegalRejected = 0;
        List<String> illegalButPassed = new ArrayList<>();
        List<String> legalButBlocked = new ArrayList<>();

        for (EtlTaskStatus from : EtlTaskStatus.values()) {
            for (EtlTaskStatus to : EtlTaskStatus.values()) {
                String taskId = seedTask(ItInfra.SOURCE_TABLE, targetTableFor(from, to));
                // G-2 前置：RUNNING → PARTIAL_SUCCESS 必须有 FAILED 分片，且 shard_total 与之和一致
                boolean partial = from == RUNNING && to == PARTIAL_SUCCESS;
                String runId = seedRun(taskId, from, 1, partial ? 1 : 0);
                if (partial) {
                    seedCheckpoint(taskId, runId, "shard-0", EtlShardStatus.FAILED, 10, 8, 2);
                }
                boolean legal = AllowedTransitions.isAllowed(from, to);
                try {
                    EtlTaskRun result = orchestrator.transition(runId, from, to,
                            ctx(taskId).reason("IT_EXHAUSTIVE").maxAttempts(orchProps.getMaxAttempts()).build());
                    if (!legal) {
                        illegalButPassed.add(from + " -> " + to);
                    } else {
                        assertThat(result.statusEnum()).isEqualTo(to);
                        assertThat(runStatus(runId)).isEqualTo(to);
                        legalExecuted++;
                    }
                } catch (IllegalStateTransitionException expectedIllegal) {
                    assertThat(legal).isFalse();
                    assertThat(runStatus(runId)).isEqualTo(from);   // 非法迁移不得触碰库
                    illegalRejected++;
                } catch (GuardFailureException guardBlocked) {
                    legalButBlocked.add(from + " -> " + to + " (" + guardBlocked.getGuard() + ")");
                }
            }
        }

        assertThat(illegalButPassed).as("迁移表外的边竟然写库成功").isEmpty();
        assertThat(legalButBlocked).as("合法边被 guard 误拦（夹具已按 §6.1 前置条件构造）").isEmpty();
        assertThat(legalExecuted).isEqualTo(32);
        assertThat(illegalRejected).isEqualTo(144 - 32);
    }

    @Test
    @DisplayName("终态保护：4 个终态只可 → SUPERSEDED，其它一律非法")
    void terminalStates_onlySuperseededIsLegal() {
        for (EtlTaskStatus terminal : List.of(SUCCEEDED, CANCELLED,
                EtlTaskStatus.FAILED, SUPERSEDED)) {
            String taskId = seedTask(ItInfra.SOURCE_TABLE, targetTableFor(terminal, QUEUED));
            String runId = seedRun(taskId, terminal);
            for (EtlTaskStatus to : EtlTaskStatus.values()) {
                if (to == SUPERSEDED && terminal != SUPERSEDED) {
                    orchestrator.transition(runId, terminal, SUPERSEDED,
                            ctx(taskId).reason("IT_TERMINAL").build());
                    assertThat(runStatus(runId)).isEqualTo(SUPERSEDED);
                    continue;
                }
                assertThatThrownBy(() -> orchestrator.transition(runId, terminal, to,
                        ctx(taskId).reason("IT_TERMINAL").build()))
                        .as("%s 的出边只允许 SUPERSEDED，实际放过了 %s", terminal, to)
                        .isInstanceOf(IllegalStateTransitionException.class);
            }
        }
    }

    @Test
    @DisplayName("禁止清单 #2：NEEDS_ATTENTION 不许被粉饰成 SUCCEEDED")
    void needsAttentionCannotBeWhitewashedIntoSucceeded() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_na");
        String runId = seedRun(taskId, NEEDS_ATTENTION);

        assertThatThrownBy(() -> orchestrator.transition(runId, NEEDS_ATTENTION, SUCCEEDED,
                ctx(taskId).reason("IT_WHITEWASH").build()))
                .isInstanceOf(IllegalStateTransitionException.class);
        assertThat(runStatus(runId)).isEqualTo(NEEDS_ATTENTION);
        // 人工出边只有 /retry(→QUEUED)、/resume(→RUNNING)、/abort(→FAILED)
        assertThat(AllowedTransitions.successors(NEEDS_ATTENTION))
                .containsExactlyInAnyOrder(QUEUED, RUNNING, EtlTaskStatus.FAILED);
    }

    @Test
    @DisplayName("CAS 并发：10 线程抢 T7 与 T8，恰一次迁移成功、其余幂等忽略")
    void casRace_tenThreadsExactlyOneTransitionWins() throws Exception {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_cas");
        String runId = seedDispatchedRun(taskId, Duration.ofSeconds(1), 1);

        double beforeSuccess = counter("etl.transition.total", "from", "DISPATCHED", "to", "RUNNING")
                + counter("etl.transition.total", "from", "DISPATCHED", "to", "QUEUED");
        double beforeIgnored = counter("etl.transition.ignored.total", "from", "DISPATCHED", "to", "RUNNING")
                + counter("etl.transition.ignored.total", "from", "DISPATCHED", "to", "QUEUED");

        int racers = 10;
        ExecutorService pool = Executors.newFixedThreadPool(racers);
        CountDownLatch ready = new CountDownLatch(racers);
        CountDownLatch go = new CountDownLatch(1);
        AtomicInteger guardBlocked = new AtomicInteger();
        List<Future<?>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < racers; i++) {
                boolean heartbeat = i % 2 == 0;   // 5 个跑 T7，5 个跑 T8
                futures.add(pool.submit(() -> {
                    ready.countDown();
                    go.await();
                    try {
                        orchestrator.transition(runId, DISPATCHED, heartbeat ? RUNNING : QUEUED,
                                ctx(taskId).reason("IT_RACE").maxAttempts(orchProps.getMaxAttempts()).build());
                    } catch (GuardFailureException e) {
                        guardBlocked.incrementAndGet();   // T8 撞上刚落地的 RUNNING 分片，合法跳过
                    }
                    return null;
                }));
            }
            assertThat(ready.await(10, TimeUnit.SECONDS)).isTrue();
            go.countDown();
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }

        assertThat(runStatus(runId)).isIn(RUNNING, QUEUED);
        assertThat(counter("etl.transition.total", "from", "DISPATCHED", "to", "RUNNING")
                + counter("etl.transition.total", "from", "DISPATCHED", "to", "QUEUED") - beforeSuccess)
                .as("CAS 下至多一次迁移真正落地").isEqualTo(1.0);
        // INV-7 侧影：attempt_no 只可能被 T8 那一次自增
        assertThat(attemptNo(runId)).isBetween(1, 2);
        if (runStatus(runId) == QUEUED) {
            assertThat(attemptNo(runId)).isEqualTo(2);
        }
        assertThat(guardBlocked.get()).isZero();
    }

    @Test
    @DisplayName("INV-1 + INV-7：同 task 的第二个非终态当前 run 被部分唯一索引拦下")
    void inv1AndInv7_secondConcurrentRunForSameTaskIsRejected() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_inv1");

        EtlTaskRun first = orchestrator.createRun(taskId);
        assertThat(first.statusEnum()).isEqualTo(EtlTaskStatus.CREATED);

        assertThatThrownBy(() -> orchestrator.createRun(taskId))
                .as("ux_run_task_current / ux_run_task_attempt 必须由 DB 强制")
                .isInstanceOf(DataAccessException.class);

        Integer active = jdbc.queryForObject("SELECT COUNT(*) FROM etl_task_run "
                        + "WHERE task_id = ? AND is_current AND status NOT IN "
                        + "('SUCCEEDED','FAILED','CANCELLED','SUPERSEDED')",
                Integer.class, taskId);
        assertThat(active).isEqualTo(1);
    }

    @Test
    @DisplayName("INV-6：etl_task.status 恒等于 current run.status（含 SUPERSEDED 后不再刷新）")
    void inv6_taskProjectionMirrorsRunStatus() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_inv6");
        EtlTaskRun run = orchestrator.createRun(taskId);
        assertThat(taskStatus(taskId)).isEqualTo("CREATED");

        orchestrator.transition(run.getRunId(), EtlTaskStatus.CREATED, EtlTaskStatus.VALIDATED,
                ctx(taskId).reason("IT_INV6").build());
        assertThat(taskStatus(taskId)).isEqualTo("VALIDATED");
        assertThat(runColumn(run.getRunId(), "status")).isEqualTo("VALIDATED");

        orchestrator.transition(run.getRunId(), EtlTaskStatus.VALIDATED, QUEUED,
                ctx(taskId).reason("IT_INV6").build());
        assertThat(taskStatus(taskId)).isEqualTo("QUEUED");

        // 新 run 取代旧终态 run 后，投影跟随新 run（旧 run 归档为 SUPERSEDED 且 is_current=FALSE）
        orchestrator.transition(run.getRunId(), QUEUED, DISPATCHED, ctx(taskId).reason("IT_INV6").build());
        orchestrator.transition(run.getRunId(), DISPATCHED, RUNNING, ctx(taskId).reason("IT_INV6").build());
        orchestrator.transition(run.getRunId(), RUNNING, SUCCEEDED, ctx(taskId).reason("IT_INV6").build());
        assertThat(taskStatus(taskId)).isEqualTo("SUCCEEDED");

        EtlTaskRun next = orchestrator.createRun(taskId);
        assertThat(runColumn(run.getRunId(), "status")).isEqualTo("SUPERSEDED");
        assertThat(jdbc.queryForObject("SELECT is_current FROM etl_task_run WHERE run_id = ?",
                Boolean.class, run.getRunId())).isFalse();
        assertThat(taskStatus(taskId)).isEqualTo("CREATED");
        assertThat(next.getTaskId()).isEqualTo(taskId);
    }

    @Test
    @DisplayName("非法迁移：计数 +1 且库中原样（P5 与 CAS 失败严格区分）")
    void illegalTransition_countsMetricAndLeavesRowUntouched() {
        String taskId = seedTask(ItInfra.SOURCE_TABLE, "it_target_illegal");
        String runId = seedRun(taskId, RUNNING);
        double before = counter("etl.transition.illegal.total", "from", "RUNNING", "to", "CREATED");

        assertThatThrownBy(() -> orchestrator.transition(runId, RUNNING, EtlTaskStatus.CREATED,
                ctx(taskId).reason("IT_ILLEGAL").build()))
                .isInstanceOf(IllegalStateTransitionException.class);

        assertThat(counter("etl.transition.illegal.total", "from", "RUNNING", "to", "CREATED") - before)
                .isEqualTo(1.0);
        assertThat(runStatus(runId)).isEqualTo(RUNNING);
        assertThat(NEEDS_ATTENTION).isNotEqualTo(runStatus(runId));
    }

    private static String targetTableFor(EtlTaskStatus from, EtlTaskStatus to) {
        return "it_target_" + from.name().toLowerCase() + "_" + to.name().toLowerCase();
    }
}
