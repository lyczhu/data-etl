package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import org.springframework.stereotype.Component;

/**
 * {@link ShardRangeResolver} 的默认实现：不计算主键区间，返回 {@link ShardBounds#empty()}。
 *
 * <p>派工消息因此不带 {@code pkFrom/pkTo}，worker 端依 checkpoint 的 {@code resumeCursor} 续跑
 * （见 {@code worker.model.ShardDispatch#toShard}）。真实 JDBC 分片区间解析依赖 {@code etl-connector-spi}
 * 的 {@code planShards} 与 validate/V-4 校验链，属后续切片；本实现保证 Phase 1 编排侧可独立编译与单测。
 */
@Component
public class NullShardRangeResolver implements ShardRangeResolver {

    @Override
    public ShardBounds resolve(EtlTaskRun run, EtlTaskCheckpoint pending) {
        return ShardBounds.empty();
    }
}
