package com.lawyus.dataetl.worker.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * worker 运行时参数（{@code etl.worker.*}）。chunk/fetch 默认对齐方案 §5.2（默认 5000，PostGIS 场景调小）。
 */
@ConfigurationProperties(prefix = "etl.worker")
public class WorkerProperties {

    /** 每 chunk 行数：读取累积到该值即提交一次（目标写 + checkpoint）。 */
    private int chunkSize = 5000;
    /** JDBC 流式游标 fetchSize。 */
    private int fetchSize = 5000;
    /** 分片总数上限（与 {@code etl.dispatch.shard-max-total} 一致）。 */
    private int maxShards = 5000;
    /** progress 上报最小间隔（毫秒），对应"每 shard ≤1/5s"。 */
    private long progressIntervalMs = 5000;
    /** 转换规则链档位（{@code JDBC}/{@code CSV}/{@code JSON}/{@code MONGO}/{@code GEOJSON}），未知回退 JDBC。 */
    private String transformProfile = "JDBC";
    /** 数值列保留小数位（{@code NumberScaleTransformer}）。 */
    private int numberScale = 2;
    /** 字段白名单（非空则前置裁剪 + snake_case 归一）；为空不裁剪。 */
    private List<String> keepColumns = new ArrayList<>();

    public int getChunkSize() { return chunkSize; }
    public void setChunkSize(int chunkSize) { this.chunkSize = chunkSize; }
    public int getFetchSize() { return fetchSize; }
    public void setFetchSize(int fetchSize) { this.fetchSize = fetchSize; }
    public int getMaxShards() { return maxShards; }
    public void setMaxShards(int maxShards) { this.maxShards = maxShards; }
    public long getProgressIntervalMs() { return progressIntervalMs; }
    public void setProgressIntervalMs(long progressIntervalMs) { this.progressIntervalMs = progressIntervalMs; }
    public String getTransformProfile() { return transformProfile; }
    public void setTransformProfile(String transformProfile) { this.transformProfile = transformProfile; }
    public int getNumberScale() { return numberScale; }
    public void setNumberScale(int numberScale) { this.numberScale = numberScale; }
    public List<String> getKeepColumns() { return keepColumns; }
    public void setKeepColumns(List<String> keepColumns) { this.keepColumns = keepColumns; }
}
