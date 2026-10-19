package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

import com.lawyus.dataetl.taskmanagement.client.DataSource;
import com.lawyus.dataetl.taskmanagement.client.DataSourceServiceClient;
import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Locale;

/**
 * 组装 {@link ShardDispatchCommand}：把 {@code (run, PENDING 分片)} 翻译成 worker 可消费的派工消息
 * （{@code orchestrator-design.md §7.2}）。
 *
 * <ul>
 *   <li>源/目标定位：由 {@link EtlTaskRepository} 取 {@link EtlTask}（源表/目标表/dataSourceId），
 *       再经 {@link DataSourceServiceClient} 解析连接串与凭证；</li>
 *   <li>connector 类型：按 {@link DataSource#getType()} 宽松映射为 {@code MYSQL}/{@code POSTGRES}
 *       （Phase 1 仅 JDBC 实装，其余源接入后扩展此映射）；</li>
 *   <li>主键区间：委托 {@link ShardRangeResolver}（默认空区间，worker 依 {@code resumeCursor} 续跑）。</li>
 * </ul>
 *
 * <p>解析失败（数据源不可达/任务不存在）抛异常，由 {@code DispatchService.pump} 捕获并把该 run 保持
 * {@code QUEUED}（不推进 T5），等 sweeper/下轮重试——派工是尽力而为，失败不污染状态机。
 */
@Component
public class ShardDispatchAssembler {

    private static final Logger log = LoggerFactory.getLogger(ShardDispatchAssembler.class);

    private final EtlTaskRepository taskRepository;
    private final DataSourceServiceClient dataSourceServiceClient;
    private final ShardRangeResolver rangeResolver;

    public ShardDispatchAssembler(EtlTaskRepository taskRepository,
                                  DataSourceServiceClient dataSourceServiceClient,
                                  ShardRangeResolver rangeResolver) {
        this.taskRepository = taskRepository;
        this.dataSourceServiceClient = dataSourceServiceClient;
        this.rangeResolver = rangeResolver;
    }

    public ShardDispatchCommand assemble(EtlTaskRun run, EtlTaskCheckpoint pending) {
        EtlTask task = taskRepository.findByTaskId(run.getTaskId());
        if (task == null) {
            throw new IllegalStateException("ETL task not found for run=" + run.getRunId()
                    + " taskId=" + run.getTaskId());
        }
        DataSource ds = task.getDataSourceId() == null ? null
                : dataSourceServiceClient.getDataSourceById(task.getDataSourceId());

        ShardDispatchCommand cmd = new ShardDispatchCommand();
        cmd.setTaskId(run.getTaskId());
        cmd.setRunId(run.getRunId());
        cmd.setAttemptNo(run.getAttemptNo() == null ? 1 : run.getAttemptNo());
        cmd.setShardId(pending.getShardId());
        cmd.setResumeCursor(pending.getLastCursor());
        cmd.setSourceTable(task.getSourceTableName());
        cmd.setTargetTable(task.getTargetTableName());
        cmd.setDataSourceId(task.getDataSourceId());

        ShardBounds bounds = rangeResolver.resolve(run, pending);
        if (bounds != null && !bounds.isEmpty()) {
            cmd.setPkFrom(bounds.from());
            cmd.setPkTo(bounds.to());
        }

        if (ds != null) {
            cmd.setConnectorType(mapConnector(ds.getType()));
            cmd.setSourceUrl(ds.getUrl());
            cmd.setSourceUsername(ds.getUsername());
            cmd.setSourcePassword(ds.getPassword());
            cmd.setSourceDriver(ds.getDriverClassName());
        } else {
            log.warn("dispatch assemble: data source unresolved for task={} (connector defaults POSTGRES)",
                    task.getTaskId());
        }
        return cmd;
    }

    /** 数据源类型 → connector 名（Phase 1 仅 JDBC；未知回落 POSTGRES）。可见以便单测。 */
    static String mapConnector(String dataSourceType) {
        if (dataSourceType == null) {
            return "POSTGRES";
        }
        String t = dataSourceType.toLowerCase(Locale.ROOT);
        if (t.contains("mysql") || t.contains("mariadb")) {
            return "MYSQL";
        }
        return "POSTGRES";
    }
}
