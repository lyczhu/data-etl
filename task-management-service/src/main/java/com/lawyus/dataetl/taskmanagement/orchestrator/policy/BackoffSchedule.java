package com.lawyus.dataetl.taskmanagement.orchestrator.policy;

import java.time.Duration;
import java.util.Date;
import java.util.List;

/**
 * 任务级退避计算（{@code orchestrator-design.md §7.4 / §7.8}）。纯函数、可注入 now，便于 §12.1「时间推进」单测。
 */
public final class BackoffSchedule {

    private BackoffSchedule() {
    }

    /**
     * 依据"当前 attempt 序号"（自增前，1-based）在退避序列中取对应时长，越界则取最后一档。
     *
     * @param backoff   退避序列（如 [5m,15m,1h]），null/空退化为 1 分钟
     * @param attemptNo 当前 attempt（迁移前的 run.attempt_no）
     * @param now       基准时间
     */
    public static Date nextAttemptAt(List<Duration> backoff, int attemptNo, Date now) {
        if (backoff == null || backoff.isEmpty()) {
            return new Date(now.getTime() + Duration.ofMinutes(1).toMillis());
        }
        int idx = Math.min(Math.max(attemptNo - 1, 0), backoff.size() - 1);
        return new Date(now.getTime() + backoff.get(idx).toMillis());
    }
}
