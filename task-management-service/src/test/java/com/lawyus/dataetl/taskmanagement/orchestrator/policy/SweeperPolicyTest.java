package com.lawyus.dataetl.taskmanagement.orchestrator.policy;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Date;

import static org.assertj.core.api.Assertions.assertThat;

class SweeperPolicyTest {

    private final Date now = new Date(1_700_000_000_000L);

    private static Date ago(Date now, Duration d) {
        return new Date(now.getTime() - d.toMillis());
    }

    @Test
    @DisplayName("§7.9 DISPATCHED 超时：先 quiesce；有活跃分片只补登记 RUNNING")
    void dispatchTimeout_quiesceFirst() {
        assertThat(SweeperPolicy.dispatchTimeout(1, 3, false))
                .isEqualTo(SweeperPolicy.DispatchTimeoutAction.RECOVER_RUNNING);
        assertThat(SweeperPolicy.dispatchTimeout(2, 3, false))
                .isEqualTo(SweeperPolicy.DispatchTimeoutAction.RECOVER_RUNNING);
    }

    @Test
    @DisplayName("§7.9 quiesce 成立：attempt 未耗尽回 QUEUED(T8)，耗尽进 NEEDS_ATTENTION(T9)")
    void dispatchTimeout_afterQuiesce() {
        assertThat(SweeperPolicy.dispatchTimeout(1, 3, true))
                .isEqualTo(SweeperPolicy.DispatchTimeoutAction.FALLBACK_QUEUED);
        assertThat(SweeperPolicy.dispatchTimeout(2, 3, true))
                .isEqualTo(SweeperPolicy.DispatchTimeoutAction.FALLBACK_QUEUED);
        assertThat(SweeperPolicy.dispatchTimeout(3, 3, true))
                .isEqualTo(SweeperPolicy.DispatchTimeoutAction.NEEDS_ATTENTION);
    }

    @Test
    @DisplayName("T21 退避到期：next_attempt_at <= now 才放行")
    void isDue() {
        assertThat(SweeperPolicy.isDue(null, now)).isFalse();
        assertThat(SweeperPolicy.isDue(ago(now, Duration.ofMinutes(1)), now)).isTrue();
        assertThat(SweeperPolicy.isDue(new Date(now.getTime() + 1000), now)).isFalse();
    }

    @Test
    @DisplayName("T16 取消 ack 超时")
    void cancelTimedOut() {
        assertThat(SweeperPolicy.isCancelTimedOut(null, now, Duration.ofSeconds(90))).isFalse();
        assertThat(SweeperPolicy.isCancelTimedOut(ago(now, Duration.ofSeconds(120)), now, Duration.ofSeconds(90))).isTrue();
        assertThat(SweeperPolicy.isCancelTimedOut(ago(now, Duration.ofSeconds(30)), now, Duration.ofSeconds(90))).isFalse();
    }

    @Test
    @DisplayName("T13 卡死：取心跳/checkpoint/started_at 的较晚活动，超阈才判卡")
    void stalled() {
        // 有心跳但已过期
        assertThat(SweeperPolicy.isStalled(ago(now, Duration.ofMinutes(40)), null, ago(now, Duration.ofHours(2)),
                now, Duration.ofMinutes(30))).isTrue();
        // checkpoint 刚更新（比 last_progress 新）→ 不算卡
        assertThat(SweeperPolicy.isStalled(ago(now, Duration.ofMinutes(40)), ago(now, Duration.ofMinutes(2)),
                ago(now, Duration.ofHours(2)), now, Duration.ofMinutes(30))).isFalse();
        // 无任何活动样本，用 started_at 兜底
        assertThat(SweeperPolicy.isStalled(null, null, ago(now, Duration.ofMinutes(45)),
                now, Duration.ofMinutes(30))).isTrue();
        // 完全无基线 → 不判卡
        assertThat(SweeperPolicy.isStalled(null, null, null, now, Duration.ofMinutes(30))).isFalse();
    }

    @Test
    @DisplayName("T32 饥饿：SCHEDULED 用 5m、THROTTLED 用 30m")
    void starved() {
        Duration sched = Duration.ofMinutes(5);
        Duration thr = Duration.ofMinutes(30);
        assertThat(SweeperPolicy.isStarved("SCHEDULED", ago(now, Duration.ofMinutes(6)), now, sched, thr)).isTrue();
        assertThat(SweeperPolicy.isStarved("SCHEDULED", ago(now, Duration.ofMinutes(2)), now, sched, thr)).isFalse();
        // THROTTLED 驻留 10m 未达 30m → 不饥饿
        assertThat(SweeperPolicy.isStarved("THROTTLED", ago(now, Duration.ofMinutes(10)), now, sched, thr)).isFalse();
        assertThat(SweeperPolicy.isStarved("THROTTLED", ago(now, Duration.ofMinutes(31)), now, sched, thr)).isTrue();
        assertThat(SweeperPolicy.isStarved(null, ago(now, Duration.ofMinutes(6)), now, sched, thr)).isTrue(); // 默认走 SCHEDULED 阈值
        assertThat(SweeperPolicy.isStarved("SCHEDULED", null, now, sched, thr)).isFalse();
    }

    @Test
    @DisplayName("T30 VALIDATED 内部错误驻留判定")
    void validatedStuck() {
        assertThat(SweeperPolicy.isValidatedStuck(ago(now, Duration.ofSeconds(15)), now, Duration.ofSeconds(10))).isTrue();
        assertThat(SweeperPolicy.isValidatedStuck(ago(now, Duration.ofSeconds(5)), now, Duration.ofSeconds(10))).isFalse();
        assertThat(SweeperPolicy.isValidatedStuck(null, now, Duration.ofSeconds(10))).isFalse();
    }
}
