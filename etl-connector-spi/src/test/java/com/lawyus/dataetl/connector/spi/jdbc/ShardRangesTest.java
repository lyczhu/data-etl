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

    @Test
    @DisplayName("按并行度切：小表按 span 收敛，不产生超过行数的空片")
    void splitByParallelismSmallTable() {
        List<long[]> r = ShardRanges.splitByParallelism(1, 20, 16, 5000);
        assertThat(r).hasSize(10);   // ceil(20/16)=2 行/片 → 10 片
        assertThat(r.get(0)).containsExactly(1, 2);
        assertThat(r.get(r.size() - 1)).containsExactly(19, 20);
        assertContiguous(r, 1, 20);
    }

    @Test
    @DisplayName("按并行度切：大表默认 16 片，chunk 只作分片内事务单元")
    void splitByParallelismLargeTable() {
        List<long[]> r = ShardRanges.splitByParallelism(1, 30_000_000, 16, 5000);
        assertThat(r).hasSize(16);
        assertThat(r.get(0)).containsExactly(1, 1_875_000);
        assertThat(r.get(15)).containsExactly(28_125_001, 30_000_000);
        assertContiguous(r, 1, 30_000_000);
    }

    @Test
    @DisplayName("按并行度切：desiredShards 超过 span 时自动收敛")
    void splitByParallelismMoreShardsThanRows() {
        List<long[]> r = ShardRanges.splitByParallelism(1, 5, 100, 5000);
        assertThat(r).hasSize(5);
        for (int i = 0; i < 5; i++) {
            assertThat(r.get(i)).containsExactly(i + 1, i + 1);
        }
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
