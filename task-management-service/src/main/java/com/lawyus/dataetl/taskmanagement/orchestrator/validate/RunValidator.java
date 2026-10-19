package com.lawyus.dataetl.taskmanagement.orchestrator.validate;

import com.lawyus.dataetl.connector.spi.model.Shard;
import com.lawyus.dataetl.connector.spi.model.ShardPlan;
import com.lawyus.dataetl.connector.spi.model.TableSchema;
import com.lawyus.dataetl.taskmanagement.client.DataSource;
import com.lawyus.dataetl.taskmanagement.client.DataSourceServiceClient;
import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.ErrorClass;
import com.lawyus.dataetl.taskmanagement.orchestrator.GuardFailureException;
import com.lawyus.dataetl.taskmanagement.orchestrator.IllegalStateTransitionException;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.DispatchProperties;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * 提交校验与入队（{@code orchestrator-design.md §7.1}）：对 {@code CREATED} run 依次执行 V-1..V-4 校验，
 * 通过后落 T2 副作用（分片总数 + 批量 PENDING checkpoint，含主键区间）并连续迁移
 * {@code CREATED → VALIDATED → QUEUED}；任一校验失败走 T3 {@code CREATED → FAILED}（{@code FATAL_CONFIG}）。
 *
 * <p>校验：
 * <ul>
 *   <li>V-1 数据源存在且可达：{@link DataSourceServiceClient} 解析 {@link DataSource}；</li>
 *   <li>V-2 源表可读且 connector 已注册：{@link ShardPlanner} 经 SPI 查找并 {@code readSchema}（隐含在 planShards）；</li>
 *   <li>V-3 目标表可建/可写：{@link TargetTableManager} 在目标库 advisory lock 下一次性建表，
 *       存量表补 {@code idempotency_key} 唯一键（WP-13；worker 不再每分片建表）；</li>
 *   <li>V-4 分片计划数 ∈ [1, {@code shard-max-total}]。</li>
 * </ul>
 *
 * <p>本方法<b>非</b>整体事务：计划落库经 {@link RunPlanWriter} 独立事务，各 transition 亦各自事务，避免某步
 * 抛错把已成功提交的计划连带回滚。仅由 {@code etl.routing.newWorker=true} 的提交路径调用。
 */
@Service
public class RunValidator {

    private static final Logger log = LoggerFactory.getLogger(RunValidator.class);

    private final EtlTaskRepository taskRepository;
    private final DataSourceServiceClient dataSourceServiceClient;
    private final ShardPlanner shardPlanner;
    private final RunPlanWriter planWriter;
    private final OrchestratorService orchestrator;
    private final DispatchProperties dispatchProperties;
    private final TargetTableManager targetTableManager;

    public RunValidator(EtlTaskRepository taskRepository,
                        DataSourceServiceClient dataSourceServiceClient,
                        ShardPlanner shardPlanner,
                        RunPlanWriter planWriter,
                        OrchestratorService orchestrator,
                        DispatchProperties dispatchProperties,
                        TargetTableManager targetTableManager) {
        this.taskRepository = taskRepository;
        this.dataSourceServiceClient = dataSourceServiceClient;
        this.shardPlanner = shardPlanner;
        this.planWriter = planWriter;
        this.orchestrator = orchestrator;
        this.dispatchProperties = dispatchProperties;
        this.targetTableManager = targetTableManager;
    }

    public void validateAndQueue(String taskId, String runId) {
        EtlTask task = taskRepository.findByTaskId(taskId);
        if (task == null) {
            fail(runId, taskId, "TASK_NOT_FOUND");
            return;
        }
        // V-1 数据源存在且可达
        DataSource ds = task.getDataSourceId() == null ? null
                : dataSourceServiceClient.getDataSourceById(task.getDataSourceId());
        if (ds == null) {
            fail(runId, taskId, "DATASOURCE_UNREACHABLE");
            return;
        }
        // V-2 源表可读 + connector 已注册；V-4 分片计划可生成（同一 SPI 调用产出）
        ShardPlan plan;
        try {
            plan = shardPlanner.plan(ds, task.getSourceTableName());
        } catch (RuntimeException planFailure) {
            fail(runId, taskId, "SOURCE_UNREADABLE_OR_UNREGISTERED: " + planFailure.getMessage());
            return;
        }
        int shardCount = plan.shardCount();
        if (shardCount < 1 || shardCount > dispatchProperties.getShardMaxTotal()) {
            fail(runId, taskId, "SHARD_PLAN_OUT_OF_RANGE(" + shardCount + ")");
            return;
        }
        // V-3 目标表可建/可写（WP-13）：目标库 advisory lock 下一次性建表；存量表补幂等键。
        if (task.getTargetTableName() == null || task.getTargetTableName().isBlank()) {
            fail(runId, taskId, "TARGET_TABLE_MISSING");
            return;
        }
        TableSchema sourceSchema;
        try {
            sourceSchema = shardPlanner.readSchema(ds, task.getSourceTableName());
        } catch (RuntimeException schemaFailure) {
            fail(runId, taskId, "SOURCE_SCHEMA_UNREADABLE: " + schemaFailure.getMessage());
            return;
        }
        try {
            targetTableManager.ensure(task, sourceSchema);
        } catch (RuntimeException targetFailure) {
            fail(runId, taskId, "TARGET_UNWRITABLE: " + targetFailure.getMessage());
            return;
        }

        // T2 副作用：分片总数 + 批量 PENDING checkpoint（携带主键闭区间）
        List<EtlTaskCheckpoint> rows = new ArrayList<>(shardCount);
        for (Shard shard : plan.shards()) {
            rows.add(EtlTaskCheckpoint.newPending(taskId, runId, shard.shardId(), shard.pkFrom(), shard.pkTo()));
        }
        planWriter.writeShardPlan(runId, shardCount, rows);

        // T2 + T4：CREATED → VALIDATED → QUEUED（各自事务，CAS 幂等）
        transition(runId, taskId, EtlTaskStatus.CREATED, EtlTaskStatus.VALIDATED, "VALIDATED", null);
        transition(runId, taskId, EtlTaskStatus.VALIDATED, EtlTaskStatus.QUEUED, "ENQUEUED", null);
        log.info("validate ok: task={} run={} shards={} -> QUEUED", taskId, runId, shardCount);
    }

    private void fail(String runId, String taskId, String reason) {
        log.warn("validate failed: task={} run={} reason={}", taskId, runId, reason);
        transition(runId, taskId, EtlTaskStatus.CREATED, EtlTaskStatus.FAILED, reason, ErrorClass.FATAL_CONFIG);
    }

    private void transition(String runId, String taskId, EtlTaskStatus from, EtlTaskStatus to,
                            String reason, ErrorClass errorClass) {
        TransitionContext ctx = TransitionContext.builder(taskId)
                .reason(reason).actor("orchestrator:validate").errorClass(errorClass).build();
        try {
            orchestrator.transition(runId, from, to, ctx);
        } catch (IllegalStateTransitionException | GuardFailureException e) {
            // run 可能已被并发/重放推进（如已非 CREATED）：幂等忽略，交由 sweeper/T31 重放
            log.warn("validate transition skipped run={} {}->{} cause={}", runId, from, to, e.getMessage());
        }
    }
}
