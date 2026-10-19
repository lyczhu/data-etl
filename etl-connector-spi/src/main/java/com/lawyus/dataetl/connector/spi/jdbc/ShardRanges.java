package com.lawyus.dataetl.connector.spi.jdbc;

import java.util.ArrayList;
import java.util.List;

/**
 * 整型主键区间分片（方案 §5.2）：<b>纯函数、可单测</b>，不触库。
 *
 * <p>给定 {@code [minPk, maxPk]}（含端点）、每片目标行数 {@code chunkSize}、分片上限 {@code maxShards}，
 * 产出若干个<b>闭区间</b> {@code [from,to]}，覆盖全部主键且互不重叠。分片数 = {@code min(maxShards, ceil(span/chunkSize))}，
 * 步长向上取整以尽量少分片。
 */
public final class ShardRanges {

    private ShardRanges() {
    }

    /**
     * @return 每个元素为 {@code long[]{from, to}}（闭区间，from&lt;=to）；{@code maxPk<minPk} 时返回空列表。
     */
    public static List<long[]> split(long minPk, long maxPk, int chunkSize, int maxShards) {
        List<long[]> ranges = new ArrayList<>();
        if (maxPk < minPk) {
            return ranges;
        }
        int chunk = Math.max(1, chunkSize);
        int cap = Math.max(1, maxShards);
        long span = maxPk - minPk + 1;
        long byChunk = (span + chunk - 1) / chunk;            // ceil(span/chunk)
        int numShards = (int) Math.min(cap, Math.max(1, byChunk));
        long step = (span + numShards - 1) / numShards;        // ceil(span/numShards)
        for (long from = minPk; from <= maxPk; ) {
            long to = Math.min(from + step - 1, maxPk);
            ranges.add(new long[]{from, to});
            from = to + 1;
        }
        return ranges;
    }
}
