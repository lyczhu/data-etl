package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.config.DispatchProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class TokenBucketTest {

    private static DispatchProperties capacity(int n) {
        DispatchProperties p = new DispatchProperties();
        p.setMaxConcurrentShards(n);
        return p;
    }

    @Test
    @DisplayName("容量封顶，all-or-nothing 申请，归还不超发")
    void acquireRelease() {
        TokenBucket bucket = new TokenBucket(capacity(3));
        assertThat(bucket.available()).isEqualTo(3);

        assertThat(bucket.tryAcquire(2)).isTrue();
        assertThat(bucket.available()).isEqualTo(1);

        // 只剩 1，申请 2 应整体失败且不留部分占用
        assertThat(bucket.tryAcquire(2)).isFalse();
        assertThat(bucket.available()).isEqualTo(1);

        assertThat(bucket.tryAcquire(1)).isTrue();
        assertThat(bucket.available()).isEqualTo(0);

        bucket.release(2);
        assertThat(bucket.available()).isEqualTo(2);
    }

    @Test
    @DisplayName("permits<=0 恒成功、不改变可用数")
    void zeroPermits() {
        TokenBucket bucket = new TokenBucket(capacity(2));
        assertThat(bucket.tryAcquire(0)).isTrue();
        assertThat(bucket.tryAcquire(-1)).isTrue();
        bucket.release(0);
        assertThat(bucket.available()).isEqualTo(2);
    }

    @Test
    @DisplayName("非法容量（负数）退化为 0 许可")
    void negativeCapacity() {
        assertThat(new TokenBucket(capacity(-5)).available()).isZero();
    }
}
