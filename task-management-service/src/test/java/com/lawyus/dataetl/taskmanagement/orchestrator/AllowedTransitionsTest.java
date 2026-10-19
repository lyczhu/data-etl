package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.orchestrator.AllowedTransitions.Edge;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.CANCELLED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.CREATED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.DISPATCHED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.FAILED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.NEEDS_ATTENTION;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.PARTIAL_SUCCESS;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.QUEUED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RETRYING;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.RUNNING;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.SUCCEEDED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.SUPERSEDED;
import static com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus.VALIDATED;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * 穷举迁移单测（{@code orchestrator-design.md §12.1}）。纯逻辑、无需 DB/Kafka。
 *
 * <p>核心断言：12×12 = 144 组合中<b>恰 32 条</b>合法边（§5.1 T2–T32，扣除创建/标记更新/重放），
 * 且迁移表 {@link AllowedTransitions} 与逐条枚举的期望边集完全一致；终态保护与禁止清单逐条校验。
 */
class AllowedTransitionsTest {

    /** 直接从 §5.1 迁移全表逐条枚举的期望合法边（评审基线，独立于被测的 AllowedTransitions）。 */
    private static Set<Edge> expectedEdges() {
        Set<Edge> e = new LinkedHashSet<>();
        // CREATED
        e.add(new Edge(CREATED, VALIDATED));   // T2/T31
        e.add(new Edge(CREATED, FAILED));      // T3
        e.add(new Edge(CREATED, CANCELLED));   // T27
        // VALIDATED
        e.add(new Edge(VALIDATED, QUEUED));    // T4
        e.add(new Edge(VALIDATED, FAILED));    // T30
        e.add(new Edge(VALIDATED, CANCELLED)); // T27
        // QUEUED
        e.add(new Edge(QUEUED, DISPATCHED));       // T5
        e.add(new Edge(QUEUED, CANCELLED));        // T27
        e.add(new Edge(QUEUED, NEEDS_ATTENTION));  // T32
        // DISPATCHED
        e.add(new Edge(DISPATCHED, RUNNING));        // T7
        e.add(new Edge(DISPATCHED, QUEUED));         // T8
        e.add(new Edge(DISPATCHED, NEEDS_ATTENTION));// T9
        e.add(new Edge(DISPATCHED, CANCELLED));      // T27
        // RUNNING
        e.add(new Edge(RUNNING, SUCCEEDED));        // T10
        e.add(new Edge(RUNNING, PARTIAL_SUCCESS));  // T11
        e.add(new Edge(RUNNING, RETRYING));         // T12
        e.add(new Edge(RUNNING, NEEDS_ATTENTION));  // T13
        e.add(new Edge(RUNNING, FAILED));           // T14
        e.add(new Edge(RUNNING, CANCELLED));        // T15/T16/T27
        // PARTIAL_SUCCESS
        e.add(new Edge(PARTIAL_SUCCESS, RETRYING));       // T17
        e.add(new Edge(PARTIAL_SUCCESS, NEEDS_ATTENTION));// T18
        e.add(new Edge(PARTIAL_SUCCESS, SUCCEEDED));      // T19
        e.add(new Edge(PARTIAL_SUCCESS, CANCELLED));      // T20/T27
        // RETRYING
        e.add(new Edge(RETRYING, DISPATCHED));       // T21
        e.add(new Edge(RETRYING, NEEDS_ATTENTION));  // T22
        e.add(new Edge(RETRYING, CANCELLED));        // T23/T27
        // NEEDS_ATTENTION
        e.add(new Edge(NEEDS_ATTENTION, QUEUED));    // T24
        e.add(new Edge(NEEDS_ATTENTION, RUNNING));   // T25
        e.add(new Edge(NEEDS_ATTENTION, FAILED));    // T26
        // 终态仅可被取代
        e.add(new Edge(SUCCEEDED, SUPERSEDED));      // T28
        e.add(new Edge(FAILED, SUPERSEDED));         // T29
        e.add(new Edge(CANCELLED, SUPERSEDED));      // T29
        return e;
    }

    @Test
    @DisplayName("§12.1 穷举：144 组合恰 32 条合法，且与迁移全表逐条一致")
    void exhaustiveMatrix_hasExactly32LegalEdges() {
        Set<Edge> expected = expectedEdges();
        assertThat(expected).hasSize(32);

        int legalCount = 0;
        for (EtlTaskStatus from : EtlTaskStatus.values()) {
            for (EtlTaskStatus to : EtlTaskStatus.values()) {
                if (AllowedTransitions.isAllowed(from, to)) {
                    legalCount++;
                    assertThat(expected).contains(new Edge(from, to));
                }
            }
        }
        assertThat(legalCount).isEqualTo(32);
        // 双向一致：AllowedTransitions 的边集 == 迁移全表枚举集
        assertThat(AllowedTransitions.allowedEdges()).isEqualTo(expected);
    }

    @Test
    @DisplayName("迁移表外的组合一律非法（144-32=112 条）")
    void exhaustiveMatrix_rejectsTheRest() {
        Set<Edge> legal = AllowedTransitions.allowedEdges();
        int illegalCount = 0;
        for (EtlTaskStatus from : EtlTaskStatus.values()) {
            for (EtlTaskStatus to : EtlTaskStatus.values()) {
                if (!legal.contains(new Edge(from, to))) {
                    assertThat(AllowedTransitions.isAllowed(from, to)).isFalse();
                    illegalCount++;
                }
            }
        }
        assertThat(illegalCount).isEqualTo(144 - 32);
    }

    @Test
    @DisplayName("禁止清单 #1：终态不可回退，仅可 →SUPERSEDED")
    void terminalStates_onlyReachableToSuperseded() {
        Set<EtlTaskStatus> terminals = Set.of(SUCCEEDED, FAILED, CANCELLED, SUPERSEDED);
        for (EtlTaskStatus t : terminals) {
            for (EtlTaskStatus to : EtlTaskStatus.values()) {
                boolean allowed = AllowedTransitions.isAllowed(t, to);
                if (t == SUPERSEDED) {
                    assertThat(allowed).as("SUPERSEDED 无任何出边").isFalse();
                } else {
                    assertThat(allowed).as("%s 只可达 SUPERSEDED", t).isEqualTo(to == SUPERSEDED);
                }
            }
        }
    }

    @Test
    @DisplayName("禁止清单 #2：NEEDS_ATTENTION→SUCCEEDED 被禁（宽恕只作用于 PARTIAL_SUCCESS）")
    void needsAttentionCannotJumpToSucceeded() {
        assertThat(AllowedTransitions.isAllowed(NEEDS_ATTENTION, SUCCEEDED)).isFalse();
        // 但 PARTIAL_SUCCESS 可经 /mark-succeeded-with-data-loss 迁 SUCCEEDED
        assertThat(AllowedTransitions.isAllowed(PARTIAL_SUCCESS, SUCCEEDED)).isTrue();
    }

    @Test
    @DisplayName("禁止清单 #3/#4/#5/#6：跳派工、执行中回退排队、退避期收敛、迁回 CREATED 均被禁")
    void forbiddenShortcutsAndRetrogress() {
        assertThat(AllowedTransitions.isAllowed(CREATED, RUNNING)).isFalse();
        assertThat(AllowedTransitions.isAllowed(VALIDATED, RUNNING)).isFalse();
        assertThat(AllowedTransitions.isAllowed(QUEUED, RUNNING)).isFalse();
        assertThat(AllowedTransitions.isAllowed(RUNNING, QUEUED)).isFalse();     // 防 shard 双跑
        assertThat(AllowedTransitions.isAllowed(RETRYING, SUCCEEDED)).isFalse(); // 退避窗口内不收敛
        for (EtlTaskStatus from : EtlTaskStatus.values()) {
            assertThat(AllowedTransitions.isAllowed(from, CREATED)).as("%s→CREATED 非法", from).isFalse();
        }
    }

    @Test
    @DisplayName("禁止清单 #7：自环（含 SUCCEEDED→SUCCEEDED 重复 results）非法")
    void noSelfLoops() {
        Set<EtlTaskStatus> seen = new HashSet<>();
        for (EtlTaskStatus s : EtlTaskStatus.values()) {
            assertThat(AllowedTransitions.isAllowed(s, s)).as("%s 自环非法", s).isFalse();
            seen.add(s);
        }
        assertThat(seen).hasSize(12);
    }

    @Test
    @DisplayName("终态判定与后继集合语义")
    void terminalAndSuccessorSemantics() {
        assertThat(SUCCEEDED.isTerminal()).isTrue();
        assertThat(FAILED.isTerminal()).isTrue();
        assertThat(CANCELLED.isTerminal()).isTrue();
        assertThat(SUPERSEDED.isTerminal()).isTrue();
        assertThat(RUNNING.isTerminal()).isFalse();
        assertThat(NEEDS_ATTENTION.isTerminal()).isFalse();

        assertThat(AllowedTransitions.successors(SUPERSEDED)).isEmpty();
        assertThat(AllowedTransitions.successors(RUNNING)).containsExactlyInAnyOrder(
                SUCCEEDED, PARTIAL_SUCCESS, RETRYING, NEEDS_ATTENTION, FAILED, CANCELLED);
    }
}
