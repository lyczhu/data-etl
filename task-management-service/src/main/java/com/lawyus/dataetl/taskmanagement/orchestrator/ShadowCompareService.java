package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.RoutingProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * 影子比对（WP-16 / D12，{@code etl.routing.shadow-compare=true}）。
 *
 * <p>灰度期新旧链路可能同时留下观测值：旧链路的 {@code etl_task.completed_records} 与新链路
 * {@code etl_task_run.rows_out} 应一致。只在旧链路确实写过计数（>0）时比对，避免新链路独跑时
 * 拿 0 误报；不一致计 {@code etl.shadow.compare.mismatch.total} 并打 WARN，不改变任何状态。
 */
@Service
public class ShadowCompareService {

    private static final Logger log = LoggerFactory.getLogger(ShadowCompareService.class);

    private final EtlTaskRepository taskRepository;
    private final EtlTaskRunRepository runRepository;
    private final OrchestratorMetrics metrics;
    private final RoutingProperties routing;

    public ShadowCompareService(EtlTaskRepository taskRepository,
                                EtlTaskRunRepository runRepository,
                                OrchestratorMetrics metrics,
                                RoutingProperties routing) {
        this.taskRepository = taskRepository;
        this.runRepository = runRepository;
        this.metrics = metrics;
        this.routing = routing;
    }

    public void compare(String taskId, String runId) {
        if (!routing.isShadowCompare() || taskId == null || runId == null) {
            return;
        }
        EtlTask task = taskRepository.findByTaskId(taskId);
        EtlTaskRun run = runRepository.findByRunId(runId).orElse(null);
        if (task == null || run == null) {
            return;
        }
        long oldCompleted = task.getCompletedRecords() == null ? 0L : task.getCompletedRecords();
        if (oldCompleted <= 0) {
            return;   // 旧链路未写影子计数：没有可比对样本
        }
        long newRowsOut = run.getRowsOut() == null ? 0L : run.getRowsOut();
        if (oldCompleted != newRowsOut) {
            metrics.shadowMismatch();
            log.warn("shadow compare mismatch task={} run={} oldCompleted={} newRowsOut={}",
                    taskId, runId, oldCompleted, newRowsOut);
        }
    }
}
