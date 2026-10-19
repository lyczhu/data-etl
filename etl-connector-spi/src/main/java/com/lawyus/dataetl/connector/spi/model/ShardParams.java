package com.lawyus.dataetl.connector.spi.model;

/**
 * 分片计划参数（方案 v2 D4）：
 *
 * @param chunkSize    分片内事务单元行数（默认 5000；PostGIS 场景 2000）
 * @param maxShards    单任务分片总数上限（对应 {@code etl.dispatch.shard-max-total}）
 * @param desiredShards 期望并行分片数（8~32/任务）；分片是并行单元，chunk 才是事务单元
 */
public record ShardParams(int chunkSize, int maxShards, int desiredShards) {

    public static final int DEFAULT_DESIRED_SHARDS = 16;

    public ShardParams {
        if (chunkSize <= 0) {
            chunkSize = 5000;
        }
        if (maxShards <= 0) {
            maxShards = 5000;
        }
        if (desiredShards <= 0) {
            desiredShards = DEFAULT_DESIRED_SHARDS;
        }
    }

    /** 兼容旧 2 参调用：使用默认并行度。 */
    public ShardParams(int chunkSize, int maxShards) {
        this(chunkSize, maxShards, DEFAULT_DESIRED_SHARDS);
    }
}
