package com.lawyus.dataetl.taskmanagement.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 派工与背压参数（{@code orchestrator-design.md §7.2 / §7.8}），对应 {@code etl.dispatch} 配置段。
 */
@Component
@ConfigurationProperties(prefix = "etl.dispatch")
public class DispatchProperties {

    /** 每轮 pump 处理的 QUEUED run 上限。 */
    private int pumpBatch = 50;
    /** 单个 run 一次最多领取的并发分片许可。 */
    private int permitsPerRun = 8;
    /** 全局同时在跑分片上限（令牌桶容量），护住源库。 */
    private int maxConcurrentShards = 24;
    /** 单任务分片总数上限（超出为配置错误，T2 guard V-4）。 */
    private int shardMaxTotal = 5000;
    /** 每分片目标行数（JDBC 主键区间分片的 chunk 大小，§5.2 默认 5000）。 */
    private int shardChunkSize = 5000;
    /** 分片目标数（并行单元，方案 v2 D4；与 chunk 大小解耦，默认 16）。 */
    private int shardTargetCount = 16;

    public int getPumpBatch() {
        return pumpBatch;
    }

    public void setPumpBatch(int pumpBatch) {
        this.pumpBatch = pumpBatch;
    }

    public int getPermitsPerRun() {
        return permitsPerRun;
    }

    public void setPermitsPerRun(int permitsPerRun) {
        this.permitsPerRun = permitsPerRun;
    }

    public int getMaxConcurrentShards() {
        return maxConcurrentShards;
    }

    public void setMaxConcurrentShards(int maxConcurrentShards) {
        this.maxConcurrentShards = maxConcurrentShards;
    }

    public int getShardMaxTotal() {
        return shardMaxTotal;
    }

    public void setShardMaxTotal(int shardMaxTotal) {
        this.shardMaxTotal = shardMaxTotal;
    }

    public int getShardChunkSize() {
        return shardChunkSize;
    }

    public void setShardChunkSize(int shardChunkSize) {
        this.shardChunkSize = shardChunkSize;
    }

    public int getShardTargetCount() {
        return shardTargetCount;
    }

    public void setShardTargetCount(int shardTargetCount) {
        this.shardTargetCount = shardTargetCount;
    }
}
