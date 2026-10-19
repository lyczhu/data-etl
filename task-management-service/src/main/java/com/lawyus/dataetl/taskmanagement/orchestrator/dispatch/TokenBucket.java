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
    private final int capacity;

    public TokenBucket(DispatchProperties properties) {
        // 公平模式，容量取全局并发分片上限
        this.capacity = Math.max(0, properties.getMaxConcurrentShards());
        this.semaphore = new Semaphore(this.capacity, true);
    }

    /** 非阻塞申请 {@code permits} 个许可；不足则回滚申请并返回 false（全有或全无，保留旧语义）。 */
    public boolean tryAcquire(int permits) {
        if (permits <= 0) {
            return true;
        }
        if (semaphore.tryAcquire(permits)) {
            return true;
        }
        return false;
    }

    /**
     * 尽量申请（WP-01 / P1-6）：返回实际获得的许可数（0..desired）。
     * 不再“有 3 个空闲、需要 8 个就饿死”，令牌不足时部分派工。
     */
    public int tryAcquireUpTo(int desired) {
        if (desired <= 0) {
            return 0;
        }
        int acquired = 0;
        while (acquired < desired && semaphore.tryAcquire(1)) {
            acquired++;
        }
        return acquired;
    }

    /** 归还 {@code permits} 个许可（分片进入终态时调用）。 */
    public void release(int permits) {
        if (permits > 0) {
            semaphore.release(permits);
        }
    }

    /**
     * 归还许可但不超过容量：at-least-once 下同分片的重复 results/dlt 会多次触发归还，
     * 若直接 {@code release} 会使可用数超过 {@code maxConcurrent-shards}、破掉背压上界。
     * 此处只释放到容量上限为止（安全钳制）。返回实际归还数。
     */
    public int releaseWithinCapacity(int permits) {
        if (permits <= 0) {
            return 0;
        }
        int room = capacity - semaphore.availablePermits();
        int actual = Math.min(permits, Math.max(0, room));
        if (actual > 0) {
            semaphore.release(actual);
        }
        return actual;
    }

    /** 当前可用许可数（观测/测试用）。 */
    public int available() {
        return semaphore.availablePermits();
    }
}
