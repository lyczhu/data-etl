package com.lawyus.dataetl.taskmanagement.orchestrator.adapter;

import com.lawyus.dataetl.taskmanagement.orchestrator.ErrorClass;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;

/**
 * 旧链路终态消息（{@code task-completed} / {@code task-failed}）到状态机迁移意图的<b>纯翻译</b>
 * （{@code orchestrator-design.md §11.2}）。适配层只负责"把旧消息翻译成 (to, reason)"，<b>绝不直接存库</b>，
 * 迁移一律经 {@code OrchestratorService.transition(runId, run.statusEnum(), to, ctx)}。
 *
 * <p>映射依据 §11.1：
 * <ul>
 *   <li>{@code completed} → {@link EtlTaskStatus#SUCCEEDED}（无 checkpoint 时 G-4 由调用侧降级为行数对比，本切片不接入）；</li>
 *   <li>{@code failed} → {@link EtlTaskStatus#NEEDS_ATTENTION}（旧消息无 {@code errorClass}，"宁可多告不可漏告"）。</li>
 * </ul>
 *
 * <p>注：把现有 {@code TaskCompletedListener}/{@code TaskFailedListener} 改走本适配层，属新旧链路
 * <b>影子切换 cutover</b>（需先让 submit 建 run、并受 {@code etl.routing.newWorker} 开关约束），
 * 依赖 Worker/SPI 接线切片与运行中的 Kafka/PG，故本内核只提供翻译逻辑与单测，未改动现网监听器。
 */
public final class LegacyStatusAdapter {

    /** 旧链路终态信号。 */
    public enum LegacySignal {
        COMPLETED,
        FAILED
    }

    /** 翻译结果：目标状态 + 由谁触发（reason）。{@code from} 由调用侧用当前 run 状态填入。 */
    public record Intent(EtlTaskStatus to, String reason, ErrorClass errorClass) {
    }

    private LegacyStatusAdapter() {
    }

    public static Intent toTransition(LegacySignal signal) {
        if (signal == null) {
            throw new IllegalArgumentException("LegacySignal must not be null");
        }
        switch (signal) {
            case COMPLETED:
                return new Intent(EtlTaskStatus.SUCCEEDED, "LEGACY_COMPLETED", null);
            case FAILED:
                return new Intent(EtlTaskStatus.NEEDS_ATTENTION, "LEGACY_FAILED", null);
            default:
                throw new IllegalArgumentException("Unsupported legacy signal: " + signal);
        }
    }

    /** 从旧状态字面量解析信号；非终态字面量返回 null（调用侧据此跳过迁移）。 */
    public static LegacySignal parseLegacyStatus(String status) {
        if (status == null) {
            return null;
        }
        switch (status.toLowerCase()) {
            case "completed":
                return LegacySignal.COMPLETED;
            case "failed":
                return LegacySignal.FAILED;
            default:
                return null;
        }
    }
}
