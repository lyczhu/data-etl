package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;

/**
 * 分片主键区间解析器：把一条 {@link EtlShardStatus#PENDING} 的 checkpoint 行解析成派工所需的
 * {@link ShardBounds}（{@code orchestrator-design.md §7.2}）。
 *
 * <p>真实实现应在 validate/V-4 阶段用 {@code SourceConnector.planShards} 生成并持久化区间；本切片的默认实现
 * {@link NullShardRangeResolver} 返回空区间（派工消息不带 pk 边界，worker 依 {@code resumeCursor} 续跑），
 * 以保持编排器与 {@code etl-connector-spi} 及源库连通性的解耦、可单测。
 */
public interface ShardRangeResolver {

    ShardBounds resolve(EtlTaskRun run, EtlTaskCheckpoint pending);
}
