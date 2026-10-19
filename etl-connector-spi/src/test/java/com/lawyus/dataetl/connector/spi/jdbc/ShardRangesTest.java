package com.lawyus.dataetl.connector.spi.jdbc;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class ShardRangesTest {

    @Test
    @DisplayName("按 chunkSize 切：覆盖全区间、闭区间、不重叠")
    void coversWholeRange() {
        List<long[]> r = ShardRanges.split(1, 10, 3, 100);
        assertThat(r).hasSize(4);
        assertThat(r.get(0)).containsExactly(1, 3);
        assertThat(r.get(3)).containsExactly(10, 10);
        assertContiguous(r, 1, 10);
    }

    @Test
    @DisplayName("maxShards 上限钳制分片数")
    void clampedByMaxShards() {
        List<long[]> r = ShardRanges.split(1, 10, 1, 2); // 想按 1 切但上限 2
        assertThat(r).hasSize(2);
        assertThat(r.get(0)).containsExactly(1, 5);
        assertThat(r.get(1)).containsExactly(6, 10);
        assertContiguous(r, 1, 10);
    }

    @Test
    @DisplayName("单点：min==max 产出 1 片")
    void singlePoint() {
        List<long[]> r = ShardRanges.split(7, 7, 100, 100);
        assertThat(r).hasSize(1);
        assertThat(r.get(0)).containsExactly(7, 7);
    }

    @Test
    @DisplayName("空区间：max<min 返回空")
    void empty() {
        assertThat(ShardRanges.split(5, 4, 10, 10)).isEmpty();
    }

    @Test
    @DisplayName("非法参数被夹到 >=1，不抛异常")
    void guardsIllegalArgs() {
        List<long[]> r = ShardRanges.split(1, 4, 0, -3); // chunk->1, cap->1 → 1 片 [1,4]
        assertThat(r).hasSize(1);
        assertThat(r.get(0)).containsExactly(1, 4);
    }

    private static void assertContiguous(List<long[]> ranges, long min, long max) {
        long cur = min;
        for (long[] rg : ranges) {
            assertThat(rg[0]).isEqualTo(cur);
            assertThat(rg[1]).isGreaterThanOrEqualTo(rg[0]);
            cur = rg[1] + 1;
        }
        assertThat(cur).isEqualTo(max + 1);
    }
}
