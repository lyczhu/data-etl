package com.lawyus.dataetl.connector.spi;

import com.lawyus.dataetl.connector.spi.model.Shard;
import com.lawyus.dataetl.connector.spi.model.ShardParams;
import com.lawyus.dataetl.connector.spi.model.ShardPlan;
import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.connector.spi.model.SourceSpec;
import com.lawyus.dataetl.connector.spi.model.TableSchema;

/**
 * 统一 Source SPI（方案 §2.1）：把一种数据源抽象为"读结构 → 计划分片 → 逐片流式读"。
 *
 * <p>编排/worker 只依赖本接口，新增一类源 = 新增一个实现并在 {@link ConnectorRegistry} 注册，
 * 无需为每种源单开微服务（解决痛点 E1）。
 */
public interface SourceConnector {

    ConnectorType type();

    /** 读取源表结构（列/类型/主键）。 */
    TableSchema readSchema(SourceSpec spec);

    /** 生成分片计划（§5.2）：JDBC 按主键区间；其它按各自游标。 */
    ShardPlan planShards(SourceSpec spec, ShardParams params);

    /** 打开一个分片的游标；{@code shard.resumeCursor} 非空则从断点续读。调用方必须 close。 */
    CloseableIterator<SourceRecord> open(Shard shard, SourceSpec spec);
}
