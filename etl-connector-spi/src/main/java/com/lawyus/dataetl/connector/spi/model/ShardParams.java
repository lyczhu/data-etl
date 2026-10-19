package com.lawyus.dataetl.connector.spi.model;

/**
 * 分片计划参数（方案 §5.2）。
 *
 * @param chunkSize 每 chunk 行数（默认 5000；PostGIS 场景 2000）
 * @param maxShards 单任务分片总数上限（对应 {@code etl.dispatch.shard-max-total}）
 */
public record ShardParams(int chunkSize, int maxShards) {

    public ShardParams {
        if (chunkSize <= 0) {
            chunkSize = 5000;
        }
        if (maxShards <= 0) {
            maxShards = 5000;
        }
    }
}
