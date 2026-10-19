package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.config.DispatchProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * WP-12：许可真源是 DB 中的全局活跃分片数，而非 JVM 内的 Semaphore。
 * 这里用 mock 仓储验证“容量 - 活跃数”的边界与空操作释放语义。
 */
class TokenBucketTest {

    private EtlTaskCheckpointRepository checkpointRepository;

    @BeforeEach
    void setUp() {
        checkpointRepository = mock(EtlTaskCheckpointRepository.class);
    }

    private TokenBucket bucket(int capacity, long active) {
        DispatchProperties p = new DispatchProperties();
        p.setMaxConcurrentShards(capacity);
        when(checkpointRepository.countActiveShardsGlobal()).thenReturn(active);
        return new TokenBucket(p, checkpointRepository);
    }

    @Test
    @DisplayName("可用数 = 容量 - 全局活跃分片数；申请不预留，只做上界判断")
    void availableDerivedFromGlobalActive() {
        TokenBucket bucket = bucket(3, 0);
        assertThat(bucket.available()).isEqualTo(3);
        assertThat(bucket.tryAcquire(3)).isTrue();
        assertThat(bucket.tryAcquire(4)).isFalse();

        when(checkpointRepository.countActiveShardsGlobal()).thenReturn(2L);
        assertThat(bucket.available()).isEqualTo(1);
        assertThat(bucket.tryAcquireUpTo(5)).isEqualTo(1);
        assertThat(bucket.tryAcquire(2)).isFalse();

        when(checkpointRepository.countActiveShardsGlobal()).thenReturn(5L);
        assertThat(bucket.available()).isZero();
    }

    @Test
    @DisplayName("release 为空操作：分片终态后由 checkpoint 状态自然释放")
    void releaseIsNoOp() {
        TokenBucket bucket = bucket(2, 2);
        assertThat(bucket.available()).isZero();

        bucket.release(1);
        assertThat(bucket.available()).isZero();
        assertThat(bucket.releaseWithinCapacity(1)).isZero();
    }

    @Test
    @DisplayName("permits<=0 恒成功；非法容量退化为 0 许可")
    void zeroAndNegative() {
        TokenBucket zero = bucket(2, 2);
        assertThat(zero.tryAcquire(0)).isTrue();
        assertThat(zero.tryAcquire(-1)).isTrue();
        assertThat(zero.tryAcquireUpTo(0)).isZero();

        assertThat(bucket(-5, 0).available()).isZero();
    }
}
