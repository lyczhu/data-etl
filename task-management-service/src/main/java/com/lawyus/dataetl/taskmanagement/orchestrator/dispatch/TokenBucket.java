package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.config.DispatchProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import org.springframework.stereotype.Component;

/**
 * 全局并发分片许可（{@code orchestrator-design.md §5.4 / §7.2}，WP-12）。
 *
 * <p>与 Phase 1A 的 JVM {@code Semaphore} 不同，这里的真源是数据库中的全局活跃分片数
 * （{@code ASSIGNED/RUNNING} 的 checkpoint）。因此多个 orchestrator 实例看到的是同一个上界，
 * 不会出现“每实例各 24 个许可、合计 48”的超发。
 *
 * <p>并发安全：{@link DispatchService#dispatchOne} 在同一事务内先取 PostgreSQL advisory lock，
 * 再调用本类计算可用许可并逐片 {@code markAssigned}，使“计数 → 派工 → 置 ASSIGNED”跨实例串行。
 * {@link #release(int)} 为语义兼容的空操作：分片进入终态后 checkpoint 状态自然下降，
 * 不需要（也不能）再显式归还数据库计数。
 */
@Component
public class TokenBucket {

    private final DispatchProperties properties;
    private final EtlTaskCheckpointRepository checkpointRepository;

    public TokenBucket(DispatchProperties properties, EtlTaskCheckpointRepository checkpointRepository) {
        this.properties = properties;
        this.checkpointRepository = checkpointRepository;
    }

    /** 非阻塞申请 {@code permits} 个许可；不足返回 false（全有或全无，保留旧语义）。 */
    public boolean tryAcquire(int permits) {
        if (permits <= 0) {
            return true;
        }
        return available() >= permits;
    }

    /**
     * 尽量申请（WP-01 / P1-6）：返回实际可发放的许可数（0..desired）。
     * 不再“有 3 个空闲、需要 8 个就饿死”，令牌不足时部分派工。
     */
    public int tryAcquireUpTo(int desired) {
        if (desired <= 0) {
            return 0;
        }
        return Math.min(desired, available());
    }

    /** 兼容旧语义的空操作：许可由 checkpoint 状态自然释放（WP-12）。 */
    public void release(int permits) {
        // no-op：DB 计数在分片进入 SUCCEEDED/FAILED/SKIPPED 后自动下降
    }

    /** 兼容旧语义的空操作；返回 0 表示没有需要显式归还的令牌。 */
    public int releaseWithinCapacity(int permits) {
        return 0;
    }

    /** 当前可用许可数 = 容量 - 全局活跃分片数（观测/测试用；容量按配置动态读取）。 */
    public int available() {
        long active = checkpointRepository.countActiveShardsGlobal();
        int capacity = Math.max(0, properties.getMaxConcurrentShards());
        return (int) Math.max(0, capacity - active);
    }
}
