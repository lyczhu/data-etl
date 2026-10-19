package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import org.springframework.stereotype.Component;

/**
 * {@link ShardRangeResolver} 的 JDBC 实现：直接读取 validate/T2 阶段持久化进 checkpoint 的主键闭区间
 * {@code pk_from}/{@code pk_to}（{@code orchestrator-design.md §7.2}，DDL 06）。
 *
 * <p>派工时不再重连源库重算 MIN/MAX/COUNT——区间在计划生成时已定，pump 只搬运。非 JDBC 源（区间为空）
 * 返回 {@link ShardBounds#empty()}，worker 端依 {@code resumeCursor} 续跑。
 */
@Component
public class CheckpointShardRangeResolver implements ShardRangeResolver {

    @Override
    public ShardBounds resolve(EtlTaskRun run, EtlTaskCheckpoint pending) {
        Long from = pending.getPkFrom();
        Long to = pending.getPkTo();
        if (from == null && to == null) {
            return ShardBounds.empty();
        }
        return new ShardBounds(from, to);
    }
}
