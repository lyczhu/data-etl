package com.lawyus.dataetl.connector.spi.model;

import java.util.Collections;
import java.util.List;

/**
 * {@code planShards} 的结果：分片清单 + 预估总行数。
 */
public record ShardPlan(List<Shard> shards, long estimatedRows) {

    public ShardPlan {
        shards = shards == null ? Collections.emptyList() : Collections.unmodifiableList(shards);
    }

    public int shardCount() {
        return shards.size();
    }
}
