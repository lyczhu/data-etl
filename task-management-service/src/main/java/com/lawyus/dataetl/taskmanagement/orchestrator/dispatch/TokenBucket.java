package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.config.DispatchProperties;
import org.springframework.stereotype.Component;

import java.util.concurrent.Semaphore;

/**
 * 全局并发分片令牌桶（{@code orchestrator-design.md §5.4 / §7.2}）：容量 = {@code etl.dispatch.max-concurrent-shards}，
 * 护住源库。{@code DispatchService.pump} 在派工前按 {@code min(remainingShards, permitsPerRun)} 申请许可，
 * 申请不足则把 run 标 {@code queued_reason=THROTTLED}（T6，非状态迁移）。
 *
 * <p>公平锁（{@code fair=true}）避免长尾 run 饿死。本组件与 worker/Kafka 解耦，可独立单测。
 */
@Component
public class TokenBucket {

    private final Semaphore semaphore;

    public TokenBucket(DispatchProperties properties) {
        // 公平模式，容量取全局并发分片上限
        this.semaphore = new Semaphore(Math.max(0, properties.getMaxConcurrentShards()), true);
    }

    /** 非阻塞申请 {@code permits} 个许可；不足则回滚申请并返回 false。 */
    public boolean tryAcquire(int permits) {
        if (permits <= 0) {
            return true;
        }
        if (semaphore.tryAcquire(permits)) {
            return true;
        }
        return false;
    }

    /** 归还 {@code permits} 个许可（分片进入终态时调用）。 */
    public void release(int permits) {
        if (permits > 0) {
            semaphore.release(permits);
        }
    }

    /** 当前可用许可数（观测/测试用）。 */
    public int available() {
        return semaphore.availablePermits();
    }
}
