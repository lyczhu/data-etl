package com.lawyus.dataetl.connector.spi.model;

/**
 * 一个分片描述符（方案 §5.2）。派工消息只带 shard，不带数据行。
 *
 * <p>JDBC 走 {@code pkFrom/pkTo}（整型主键闭区间 {@code [pkFrom, pkTo]}）；
 * 非 JDBC 走 {@code cursorFrom/cursorTo} 字符串游标（Mongo _id、文件行区间+offset、stream topic-partition:offset）。
 * {@code resumeCursor} 为断点续传游标（worker 从 checkpoint 读出后回填；首跑为 null）。
 */
public record Shard(String shardId,
                    Long pkFrom,
                    Long pkTo,
                    String cursorFrom,
                    String cursorTo,
                    String resumeCursor) {

    public static Shard numeric(String shardId, long pkFrom, long pkTo) {
        return new Shard(shardId, pkFrom, pkTo, null, null, null);
    }
}
