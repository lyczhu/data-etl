package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;

/**
 * 分片主键区间解析器：把一条 {@link EtlShardStatus#PENDING} 的 checkpoint 行解析成派工所需的
 * {@link ShardBounds}（{@code orchestrator-design.md §7.2}）。
 *
 * <p>真实区间在 validate/V-4 阶段由 {@code SourceConnector.planShards} 生成并持久化进 checkpoint 的
 * {@code pk_from}/{@code pk_to}；{@link CheckpointShardRangeResolver} 直接读取这两列（派工不再重连源库）。
 * 非 JDBC 源或无区间时返回 {@link ShardBounds#empty()}，worker 端依 {@code resumeCursor} 续跑。
 */
public interface ShardRangeResolver {

    ShardBounds resolve(EtlTaskRun run, EtlTaskCheckpoint pending);
}
