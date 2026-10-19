package com.lawyus.dataetl.taskmanagement.orchestrator.validate;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskCheckpoint;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskCheckpointRepository;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * T2 副作用落库器（{@code orchestrator-design.md §7.1}）：在同一事务内把分片总数写回 run、批量 INSERT
 * {@link com.lawyus.dataetl.taskmanagement.orchestrator.EtlShardStatus#PENDING} checkpoint（含主键闭区间）。
 *
 * <p>独立成 bean 是为了让 {@code @Transactional} 经代理生效——{@link RunValidator} 本身不开事务，避免
 * "计划落库 + 后续 transition" 混在一个事务里时，某个 transition 抛错把已提交的计划一起回滚。
 */
@Service
public class RunPlanWriter {

    private final EtlTaskRunRepository runRepository;
    private final EtlTaskCheckpointRepository checkpointRepository;

    public RunPlanWriter(EtlTaskRunRepository runRepository, EtlTaskCheckpointRepository checkpointRepository) {
        this.runRepository = runRepository;
        this.checkpointRepository = checkpointRepository;
    }

    @Transactional
    public void writeShardPlan(String runId, int shardTotal, List<EtlTaskCheckpoint> pendingRows) {
        runRepository.setShardTotal(runId, shardTotal);
        if (pendingRows != null && !pendingRows.isEmpty()) {
            checkpointRepository.saveAll(pendingRows);
        }
    }
}
