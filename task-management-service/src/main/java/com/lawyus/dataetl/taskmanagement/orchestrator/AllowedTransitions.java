package com.lawyus.dataetl.taskmanagement.orchestrator;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
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

/**
 * run 层状态机的合法迁移表（G-1 ALLOWED），逐条对应 {@code orchestrator-design.md §6.2}。
 *
 * <p>该表是"transition 是唯一状态写路径"守卫（§11.3）的判定依据：迁移表之外的任意
 * {@code (from → to)} 一律视为协议被破坏，抛 {@link IllegalStateTransitionException}（P5）。
 *
 * <p>终态保护（禁止清单 #1）由表结构天然保证：{@code SUCCEEDED/FAILED/CANCELLED} 仅可达
 * {@code SUPERSEDED}，{@code SUPERSEDED} 无任何出边。
 */
public final class AllowedTransitions {

    /** 一条 from→to 迁移边。 */
    public record Edge(EtlTaskStatus from, EtlTaskStatus to) {
    }

    private static final Map<EtlTaskStatus, Set<EtlTaskStatus>> ALLOWED;

    static {
        Map<EtlTaskStatus, Set<EtlTaskStatus>> m = new LinkedHashMap<>();
        m.put(CREATED, Set.of(VALIDATED, FAILED, CANCELLED));
        m.put(VALIDATED, Set.of(QUEUED, FAILED, CANCELLED));
        m.put(QUEUED, Set.of(DISPATCHED, CANCELLED, NEEDS_ATTENTION));
        m.put(DISPATCHED, Set.of(RUNNING, QUEUED, NEEDS_ATTENTION, CANCELLED));
        m.put(RUNNING, Set.of(SUCCEEDED, PARTIAL_SUCCESS, RETRYING, NEEDS_ATTENTION, FAILED, CANCELLED));
        m.put(PARTIAL_SUCCESS, Set.of(RETRYING, NEEDS_ATTENTION, SUCCEEDED, CANCELLED));
        m.put(RETRYING, Set.of(DISPATCHED, NEEDS_ATTENTION, CANCELLED));
        m.put(NEEDS_ATTENTION, Set.of(QUEUED, RUNNING, FAILED));
        m.put(SUCCEEDED, Set.of(SUPERSEDED));
        m.put(FAILED, Set.of(SUPERSEDED));
        m.put(CANCELLED, Set.of(SUPERSEDED));
        m.put(SUPERSEDED, Set.of());

        Map<EtlTaskStatus, Set<EtlTaskStatus>> unmodifiable = new LinkedHashMap<>();
        m.forEach((k, v) -> unmodifiable.put(k, Collections.unmodifiableSet(v)));
        ALLOWED = Collections.unmodifiableMap(unmodifiable);
    }

    private AllowedTransitions() {
    }

    /** 判断 {@code from → to} 是否在迁移表内（G-1）。 */
    public static boolean isAllowed(EtlTaskStatus from, EtlTaskStatus to) {
        Set<EtlTaskStatus> targets = ALLOWED.get(from);
        return targets != null && targets.contains(to);
    }

    /** 全部合法边（供穷举单测锁定计数）。 */
    public static Set<Edge> allowedEdges() {
        Set<Edge> edges = new LinkedHashSet<>();
        ALLOWED.forEach((from, tos) -> tos.forEach(to -> edges.add(new Edge(from, to))));
        return Collections.unmodifiableSet(edges);
    }

    /** {@code from} 的合法后继集合。 */
    public static Set<EtlTaskStatus> successors(EtlTaskStatus from) {
        return ALLOWED.getOrDefault(from, Set.of());
    }
}
