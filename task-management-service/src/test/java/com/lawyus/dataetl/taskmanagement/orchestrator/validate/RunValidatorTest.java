package com.lawyus.dataetl.taskmanagement.orchestrator.validate;

import com.lawyus.dataetl.connector.spi.model.Shard;
import com.lawyus.dataetl.connector.spi.model.ShardPlan;
import com.lawyus.dataetl.taskmanagement.client.DataSource;
import com.lawyus.dataetl.taskmanagement.client.DataSourceServiceClient;
import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.OrchestratorService;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.config.DispatchProperties;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class RunValidatorTest {

    private EtlTaskRepository taskRepository;
    private DataSourceServiceClient dsClient;
    private ShardPlanner shardPlanner;
    private RunPlanWriter planWriter;
    private OrchestratorService orchestrator;
    private DispatchProperties dispatchProperties;
    private TargetTableManager targetTableManager;
    private RunValidator validator;

    @BeforeEach
    void setUp() {
        taskRepository = mock(EtlTaskRepository.class);
        dsClient = mock(DataSourceServiceClient.class);
        shardPlanner = mock(ShardPlanner.class);
        planWriter = mock(RunPlanWriter.class);
        orchestrator = mock(OrchestratorService.class);
        dispatchProperties = new DispatchProperties();   // shardMaxTotal=5000
        targetTableManager = mock(TargetTableManager.class);
        validator = new RunValidator(taskRepository, dsClient, shardPlanner, planWriter,
                orchestrator, dispatchProperties, targetTableManager);
    }

    private EtlTask task() {
        EtlTask t = new EtlTask();
        t.setTaskId("task-1");
        t.setDataSourceId(7L);
        t.setSourceTableName("src_orders");
        t.setTargetTableName("src_orders_task1");
        return t;
    }

    private DataSource ds() {
        DataSource d = new DataSource();
        d.setId(7L);
        d.setType("postgresql");
        d.setUrl("jdbc:postgresql://h/db");
        return d;
    }

    @Test
    @DisplayName("V-1..V-4 全通过：落 shard_total + PENDING（含区间），CREATED→VALIDATED→QUEUED")
    void happyPath() {
        when(taskRepository.findByTaskId("task-1")).thenReturn(task());
        when(dsClient.getDataSourceById(7L)).thenReturn(ds());
        ShardPlan plan = new ShardPlan(
                List.of(Shard.numeric("shard-0", 1, 100), Shard.numeric("shard-1", 101, 200)), 200L);
        when(shardPlanner.plan(any(), eq("src_orders"))).thenReturn(plan);

        validator.validateAndQueue("task-1", "run-1");

        verify(planWriter).writeShardPlan(eq("run-1"), eq(2), anyList());
        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.CREATED),
                eq(EtlTaskStatus.VALIDATED), any(TransitionContext.class));
        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.VALIDATED),
                eq(EtlTaskStatus.QUEUED), any(TransitionContext.class));
    }

    @Test
    @DisplayName("PENDING 行携带主键闭区间（供 pump 派工，§7.2）")
    void pendingRowsCarryBounds() {
        when(taskRepository.findByTaskId("task-1")).thenReturn(task());
        when(dsClient.getDataSourceById(7L)).thenReturn(ds());
        when(shardPlanner.plan(any(), anyString())).thenReturn(new ShardPlan(
                List.of(Shard.numeric("shard-0", 5, 50)), 46L));

        validator.validateAndQueue("task-1", "run-1");

        verify(planWriter).writeShardPlan(eq("run-1"), eq(1),
                org.mockito.ArgumentMatchers.argThat(rows -> {
                    var c = rows.get(0);
                    return c.getPkFrom() == 5L && c.getPkTo() == 50L
                            && "shard-0".equals(c.getShardId());
                }));
    }

    @Test
    @DisplayName("V-1 数据源不可达：CREATED→FAILED(FATAL_CONFIG)，不落计划、不入队")
    void datasourceUnreachableFails() {
        when(taskRepository.findByTaskId("task-1")).thenReturn(task());
        when(dsClient.getDataSourceById(7L)).thenReturn(null);

        validator.validateAndQueue("task-1", "run-1");

        verify(planWriter, never()).writeShardPlan(anyString(), anyInt(), anyList());
        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.CREATED),
                eq(EtlTaskStatus.FAILED), any(TransitionContext.class));
        verify(orchestrator, never()).transition(anyString(), eq(EtlTaskStatus.VALIDATED),
                any(), any());
    }

    @Test
    @DisplayName("V-4 空分片计划（0 片）越界：CREATED→FAILED")
    void emptyShardPlanFails() {
        when(taskRepository.findByTaskId("task-1")).thenReturn(task());
        when(dsClient.getDataSourceById(7L)).thenReturn(ds());
        when(shardPlanner.plan(any(), anyString())).thenReturn(new ShardPlan(List.of(), 0L));

        validator.validateAndQueue("task-1", "run-1");

        verify(planWriter, never()).writeShardPlan(anyString(), anyInt(), anyList());
        verify(orchestrator).transition(eq("run-1"), eq(EtlTaskStatus.CREATED),
                eq(EtlTaskStatus.FAILED), any(TransitionContext.class));
    }
}
