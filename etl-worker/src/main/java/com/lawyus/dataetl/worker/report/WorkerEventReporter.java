package com.lawyus.dataetl.worker.report;

import com.lawyus.dataetl.worker.model.ShardProgress;
import com.lawyus.dataetl.worker.model.ShardResultMessage;

/**
 * worker → orchestrator 的事件外发抽象（progress/results）。骨架用 Kafka 实现；
 * Phase 2 换 Transactional Outbox 后由同一接口切换（业务代码不感知，§6.3）。
 */
public interface WorkerEventReporter {

    void reportProgress(ShardProgress progress);

    void reportResult(ShardResultMessage result);
}
