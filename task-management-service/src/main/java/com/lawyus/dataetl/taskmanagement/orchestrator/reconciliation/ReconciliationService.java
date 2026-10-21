package com.lawyus.dataetl.taskmanagement.orchestrator.reconciliation;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskRunRepository;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

/**
 * 终态 run 对账（Phase 2D / orchestrator-design.md §12.2 check 2/3）。
 *
 * <p>口径：
 * <ul>
 *   <li>SUCCEEDED：{@code source_rows - rows_filtered - rows_failed = target_rows}；</li>
 *   <li>其它终态：{@code target_rows = checkpoint SUM(rows_out)}；</li>
 *   <li>共同：{@code target_rows = COUNT(DISTINCT idempotency_key)}，且四段平衡成立。</li>
 * </ul>
 *
 * <p>只读外部库，不在对账事务里持有长时间锁；失败按 run 记 ERROR，不重试（下一轮/人工 force 重算）。
 */
@Service
public class ReconciliationService {

    private static final Logger log = LoggerFactory.getLogger(ReconciliationService.class);
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final int MAX_PAGE_SIZE = 200;

    private final EtlReconciliationRunRepository reconciliationRepository;
    private final EtlTaskRunRepository runRepository;
    private final EtlTaskRepository taskRepository;
    private final JdbcTemplate jdbc;
    private final ReconciliationProperties properties;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;

    public ReconciliationService(EtlReconciliationRunRepository reconciliationRepository,
                                 EtlTaskRunRepository runRepository,
                                 EtlTaskRepository taskRepository,
                                 JdbcTemplate jdbc,
                                 ReconciliationProperties properties,
                                 MeterRegistry meterRegistry,
                                 ObjectMapper objectMapper) {
        this.reconciliationRepository = reconciliationRepository;
        this.runRepository = runRepository;
        this.taskRepository = taskRepository;
        this.jdbc = jdbc;
        this.properties = properties;
        this.meterRegistry = meterRegistry;
        this.objectMapper = objectMapper;
    }

    /** 对账单个 run；已存在且非 force 时直接返回历史结果。 */
    public EtlReconciliationRun reconcile(String runId, boolean force) {
        Optional<EtlReconciliationRun> existing = reconciliationRepository.findByRunId(runId);
        if (existing.isPresent() && !force) {
            return existing.get();
        }
        EtlTaskRun run = runRepository.findByRunId(runId)
                .orElseThrow(() -> new IllegalArgumentException("run not found: " + runId));
        EtlTask task = taskRepository.findByTaskId(run.getTaskId());
        if (task == null) {
            throw new IllegalArgumentException("task not found for run: " + runId);
        }
        EtlReconciliationRun row = existing.orElseGet(() -> EtlReconciliationRun.of(runId, task.getTaskId()));
        try {
            Aggregate agg = aggregate(runId);
            DataSourceInfo source = dataSource(task.getDataSourceId());
            DataSourceInfo target = task.getTargetDataSourceId() == null
                    ? source : dataSource(task.getTargetDataSourceId());

            long sourceRows = countRows(source, task.getSourceTableName());
            TargetCounts targetCounts = countTarget(target, task.getTargetTableName());

            boolean succeeded = run.statusEnum() == EtlTaskStatus.SUCCEEDED;
            long expected = succeeded
                    ? sourceRows - agg.filtered() - agg.failed()
                    : agg.rowsOut();
            long diff = targetCounts.rows() - expected;

            List<String> reasons = new ArrayList<>();
            if (diff != 0) {
                reasons.add("expected " + expected + " target rows but found " + targetCounts.rows());
            }
            if (targetCounts.rows() != targetCounts.distinctIdempotency()) {
                reasons.add("duplicate idempotency_key: rows=" + targetCounts.rows()
                        + ", distinct=" + targetCounts.distinctIdempotency());
            }
            long balance = agg.rowsOut() + agg.filtered() + agg.failed() + agg.deduped();
            if (agg.rowsIn() != balance) {
                reasons.add("four-term balance broken: rows_in=" + agg.rowsIn() + ", sum=" + balance);
            }

            row.setStatus(reasons.isEmpty() ? EtlReconciliationRun.PASS : EtlReconciliationRun.MISMATCH);
            row.setSourceRows(sourceRows);
            row.setFilteredRows(agg.filtered());
            row.setFailedRows(agg.failed());
            row.setDedupedRows(agg.deduped());
            row.setRowsIn(agg.rowsIn());
            row.setRowsOut(agg.rowsOut());
            row.setTargetRows(targetCounts.rows());
            row.setDistinctIdempotency(targetCounts.distinctIdempotency());
            row.setExpectedTargetRows(expected);
            row.setDiff(diff);
            row.setDetails(detailsJson(task, reasons, null));
            row.setCreatedAt(new java.util.Date());
        } catch (Exception e) {
            row.setStatus(EtlReconciliationRun.ERROR);
            row.setDetails(detailsJson(null, List.of(), e));
            row.setCreatedAt(new java.util.Date());
            log.warn("reconciliation: run={} failed: {}", runId, e.getMessage());
        }
        EtlReconciliationRun saved = reconciliationRepository.save(row);
        meterRegistry.counter("etl.reconciliation.evaluations", "result", saved.getStatus()).increment();
        return saved;
    }

    public Optional<EtlReconciliationRun> find(String runId) {
        return reconciliationRepository.findByRunId(runId);
    }

    public Page<EtlReconciliationRun> page(String taskId, int page, int size) {
        int safePage = Math.max(0, page);
        int safeSize = Math.min(MAX_PAGE_SIZE, Math.max(1, size));
        return reconciliationRepository.findByTaskIdOrderByIdDesc(taskId, PageRequest.of(safePage, safeSize));
    }

    private Aggregate aggregate(String runId) {
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT COALESCE(SUM(rows_in),0) AS rows_in, COALESCE(SUM(rows_out),0) AS rows_out, "
                        + "COALESCE(SUM(rows_filtered),0) AS rows_filtered, "
                        + "COALESCE(SUM(rows_failed),0) AS rows_failed, "
                        + "COALESCE(SUM(rows_deduped),0) AS rows_deduped "
                        + "FROM etl_task_checkpoint WHERE run_id = ?", runId);
        return new Aggregate(asLong(row.get("rows_in")), asLong(row.get("rows_out")),
                asLong(row.get("rows_filtered")), asLong(row.get("rows_failed")),
                asLong(row.get("rows_deduped")));
    }

    private DataSourceInfo dataSource(Long id) {
        Map<String, Object> row = jdbc.queryForMap(
                "SELECT url, username, password FROM data_source WHERE id = ?", id);
        return new DataSourceInfo((String) row.get("url"), (String) row.get("username"),
                (String) row.get("password"));
    }

    private long countRows(DataSourceInfo dataSource, String table) throws Exception {
        String safe = safeIdentifier(table);
        try (Connection conn = DriverManager.getConnection(
                dataSource.url(), dataSource.username(), dataSource.password());
             Statement st = conn.createStatement()) {
            st.setQueryTimeout(properties.getQueryTimeoutSeconds());
            try (ResultSet rs = st.executeQuery("SELECT COUNT(*) FROM " + safe)) {
                return rs.next() ? rs.getLong(1) : 0L;
            }
        }
    }

    private TargetCounts countTarget(DataSourceInfo dataSource, String table) throws Exception {
        String safe = safeIdentifier(table);
        try (Connection conn = DriverManager.getConnection(
                dataSource.url(), dataSource.username(), dataSource.password());
             Statement st = conn.createStatement()) {
            st.setQueryTimeout(properties.getQueryTimeoutSeconds());
            try (ResultSet rs = st.executeQuery(
                    "SELECT COUNT(*), COUNT(DISTINCT idempotency_key) FROM " + safe)) {
                return rs.next() ? new TargetCounts(rs.getLong(1), rs.getLong(2)) : new TargetCounts(0L, 0L);
            }
        }
    }

    private String detailsJson(EtlTask task, List<String> reasons, Exception error) {
        Map<String, Object> details = new LinkedHashMap<>();
        if (task != null) {
            details.put("sourceTable", task.getSourceTableName());
            details.put("targetTable", task.getTargetTableName());
        }
        if (!reasons.isEmpty()) {
            details.put("reasons", reasons);
        }
        if (error != null) {
            details.put("error", error.getClass().getSimpleName() + ": " + abbreviate(error.getMessage(), 400));
        }
        try {
            return objectMapper.writeValueAsString(details);
        } catch (Exception e) {
            return null;
        }
    }

    private static String safeIdentifier(String identifier) {
        if (identifier == null || !SAFE_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("unsafe table identifier: " + identifier);
        }
        return identifier;
    }

    private static long asLong(Object value) {
        return value instanceof Number ? ((Number) value).longValue() : 0L;
    }

    private static String abbreviate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() <= max ? value : value.substring(0, max);
    }

    private record Aggregate(long rowsIn, long rowsOut, long filtered, long failed, long deduped) {
    }

    private record TargetCounts(long rows, long distinctIdempotency) {
    }

    private record DataSourceInfo(String url, String username, String password) {
    }
}
