package com.lawyus.dataetl.taskmanagement.orchestrator.policy;

import java.time.Duration;
import java.util.Date;

/**
 * sweeper 判定纯函数集（{@code orchestrator-design.md §7.9}）。与仓储/Spring 解耦，可注入显式 {@code now}
 * 做时间推进单测（§12.1）。四个 sweeper 只做"取候选 + 调 transition"，判定逻辑下沉到此。
 */
public final class SweeperPolicy {

    /** DISPATCHED 超时后的动作分支。 */
    public enum DispatchTimeoutAction {
        /** quiesce 不成立（有 shard 在跑，仅心跳丢失）：不回退，CAS 补登记 RUNNING。 */
        RECOVER_RUNNING,
        /** quiesce 成立 ∧ attempt 未耗尽：T8 回 QUEUED + attempt+1。 */
        FALLBACK_QUEUED,
        /** quiesce 成立 ∧ attempt 耗尽：T9 NEEDS_ATTENTION。 */
        NEEDS_ATTENTION
    }

    private SweeperPolicy() {
    }

    /**
     * DISPATCHED 无心跳超时判定（§7.9）：<b>先做 quiesce</b>，避免"心跳丢失但 worker 实际在跑"时盲目回退造成 shard 双执行。
     */
    public static DispatchTimeoutAction dispatchTimeout(int attemptNo, int maxAttempts, boolean quiesced) {
        if (!quiesced) {
            return DispatchTimeoutAction.RECOVER_RUNNING;
        }
        return attemptNo < maxAttempts
                ? DispatchTimeoutAction.FALLBACK_QUEUED
                : DispatchTimeoutAction.NEEDS_ATTENTION;
    }

    /** RETRYING 是否已到退避到期时间（T21）。 */
    public static boolean isDue(Date nextAttemptAt, Date now) {
        return nextAttemptAt != null && !now.before(nextAttemptAt);
    }

    /** 软取消 ack 是否超时（T16，须再叠 quiesce 判定）。 */
    public static boolean isCancelTimedOut(Date cancelRequestedAt, Date now, Duration ackTimeout) {
        if (cancelRequestedAt == null) {
            return false;
        }
        return now.getTime() - cancelRequestedAt.getTime() > ackTimeout.toMillis();
    }

    /**
     * 卡死判定（T13 STALLED，§7.9）：最近活动时间（{@code last_progress_ts} 与 checkpoint 最大 {@code updated_at} 的较晚者）
     * 距今超过阈值。无活动样本时以 {@code fallbackBaseline}（run 进入 RUNNING 的时间）兜底。
     */
    public static boolean isStalled(Date lastProgressTs, Date checkpointMaxUpdatedAt, Date fallbackBaseline,
                                    Date now, Duration threshold) {
        Date last = latest(lastProgressTs, checkpointMaxUpdatedAt);
        if (last == null) {
            last = fallbackBaseline;
        }
        if (last == null) {
            return false;
        }
        return now.getTime() - last.getTime() > threshold.toMillis();
    }

    /** QUEUED 饥饿判定（T32）：按 queued_reason 选阈值比较驻留时长。 */
    public static boolean isStarved(String queuedReason, Date enteredQueuedAt, Date now,
                                    Duration scheduledTimeout, Duration throttledTimeout) {
        if (enteredQueuedAt == null) {
            return false;
        }
        Duration limit = "THROTTLED".equalsIgnoreCase(queuedReason) ? throttledTimeout : scheduledTimeout;
        return now.getTime() - enteredQueuedAt.getTime() > limit.toMillis();
    }

    /** VALIDATED 内部错误（T30）：驻留超过 validated-timeout 仍未入队。 */
    public static boolean isValidatedStuck(Date updatedAt, Date now, Duration validatedTimeout) {
        return updatedAt != null && now.getTime() - updatedAt.getTime() > validatedTimeout.toMillis();
    }

    private static Date latest(Date a, Date b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }
        return a.after(b) ? a : b;
    }
}
