package com.lawyus.dataetl.worker.report;

import com.lawyus.dataetl.worker.model.ShardDltMessage;
import com.lawyus.dataetl.worker.model.ShardProgress;
import com.lawyus.dataetl.worker.model.ShardResultMessage;

/**
 * worker → orchestrator 的控制面回报（方案 v2 WP-05/WP-15）：progress/results/dlt。
 */
public interface WorkerEventReporter {

    void reportProgress(ShardProgress progress);

    void reportResult(ShardResultMessage result);

    void reportDlt(ShardDltMessage dlt);
}
