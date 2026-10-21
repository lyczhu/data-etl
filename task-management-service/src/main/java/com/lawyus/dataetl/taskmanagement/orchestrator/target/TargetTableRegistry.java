package com.lawyus.dataetl.taskmanagement.orchestrator.target;

import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 目标表登记与清理（Phase 2E）。
 *
 * <p>登记不等于建表：V-3（{@code TargetTableManager}）仍负责一次性建表/补幂等键；
 * 本类只回答“这张表归谁”和“孤儿能否清理”。清理只接受 {@code ORPHANED}，默认 dry-run。
 */
@Service
public class TargetTableRegistry {

    private static final Logger log = LoggerFactory.getLogger(TargetTableRegistry.class);
    private static final Pattern SAFE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final int MAX_PAGE_SIZE = 200;

    private final EtlTargetTableRepository repository;
    private final JdbcTemplate jdbc;
    private final DataSource orchestratorDataSource;
    private final TargetTableProperties properties;

    public TargetTableRegistry(EtlTargetTableRepository repository,
                               JdbcTemplate jdbc,
                               DataSource orchestratorDataSource,
                               TargetTableProperties properties) {
        this.repository = repository;
        this.jdbc = jdbc;
        this.orchestratorDataSource = orchestratorDataSource;
        this.properties = properties;
    }

    /** 提交前冲突检查：同名表已归其它 task 时拒绝。 */
    public void assertAvailable(Long targetDataSourceId, String tableName, String taskId) {
        Optional<EtlTargetTable> existing = repository.findByScope(targetDataSourceId, tableName);
        if (existing.isPresent() && !existing.get().getTaskId().equals(taskId)) {
            throw new TargetTableConflictException("target table " + tableName
                    + " already belongs to task " + existing.get().getTaskId());
        }
    }

    /** 登记目标表；同一 task 重复登记按幂等处理。 */
    public EtlTargetTable register(EtlTask task) {
        requireSafeIdentifier(task.getTargetTableName());
        Optional<EtlTargetTable> existing = repository.findByScope(
                task.getTargetDataSourceId(), task.getTargetTableName());
        if (existing.isPresent()) {
            EtlTargetTable row = existing.get();
            if (!row.getTaskId().equals(task.getTaskId())) {
                throw new TargetTableConflictException("target table " + task.getTargetTableName()
                        + " already belongs to task " + row.getTaskId());
            }
            row.setLastUsedAt(new Date());
            return repository.save(row);
        }
        try {
            return repository.save(EtlTargetTable.active(task));
        } catch (DataIntegrityViolationException race) {
            // 并发登记：唯一索引兜底，再读一次确认归属
            EtlTargetTable row = repository.findByScope(task.getTargetDataSourceId(), task.getTargetTableName())
                    .orElseThrow(() -> race);
            if (!row.getTaskId().equals(task.getTaskId())) {
                throw new TargetTableConflictException("target table " + task.getTargetTableName()
                        + " already belongs to task " + row.getTaskId());
            }
            return row;
        }
    }

    public void markOrphaned(String taskId) {
        for (EtlTargetTable row : repository.findByTaskId(taskId)) {
            if (EtlTargetTable.ACTIVE.equals(row.getState())) {
                row.setState(EtlTargetTable.ORPHANED);
                repository.save(row);
            }
        }
    }

    /** 每日只标记孤儿，不删表；返回本次标记数。 */
    public int markOrphans() {
        Date cutoff = cutoff(properties.getOrphanRetentionDays());
        List<EtlTargetTable> candidates = repository.findOrphanCandidates(cutoff);
        for (EtlTargetTable row : candidates) {
            row.setState(EtlTargetTable.ORPHANED);
            repository.save(row);
        }
        return candidates.size();
    }

    public Page<EtlTargetTable> page(String state, int page, int size) {
        int safePage = Math.max(0, page);
        int safeSize = Math.min(MAX_PAGE_SIZE, Math.max(1, size));
        PageRequest request = PageRequest.of(safePage, safeSize);
        return state == null || state.isBlank()
                ? repository.findAll(request)
                : repository.findByStateOrderByIdDesc(state.toUpperCase(), request);
    }

    /**
     * 清理孤儿目标表。{@code dryRun=true} 只返回候选，不落库、不删表；
     * {@code dryRun=false} 先把候选标 ORPHANED，再逐张 DROP，成功标 PURGED。
     */
    public CleanupReport cleanup(boolean dryRun, int retentionDays) {
        Date cutoff = cutoff(retentionDays);
        List<EtlTargetTable> candidates = repository.findOrphanCandidates(cutoff);
        List<EtlTargetTable> targets = new ArrayList<>(repository.findByState(EtlTargetTable.ORPHANED));
        Set<Long> seen = new LinkedHashSet<>();
        List<String> tables = new ArrayList<>();
        for (EtlTargetTable row : targets) {
            if (seen.add(row.getId())) {
                tables.add(row.getTableName());
            }
        }
        for (EtlTargetTable row : candidates) {
            if (seen.add(row.getId())) {
                tables.add(row.getTableName());
            }
        }
        if (dryRun) {
            return new CleanupReport(true, tables.size(), 0, tables);
        }

        for (EtlTargetTable row : candidates) {
            row.setState(EtlTargetTable.ORPHANED);
            repository.save(row);
            targets.add(row);
        }
        int dropped = 0;
        List<String> errors = new ArrayList<>();
        for (EtlTargetTable row : targets) {
            try {
                drop(row);
                row.setState(EtlTargetTable.PURGED);
                row.setPurgedAt(new Date());
                repository.save(row);
                dropped++;
            } catch (Exception e) {
                errors.add(row.getTableName() + ": " + abbreviate(e.getMessage(), 160));
                log.warn("target table cleanup failed: table={} error={}",
                        row.getTableName(), e.getMessage());
            }
        }
        return new CleanupReport(false, tables.size(), dropped, errors);
    }

    private void drop(EtlTargetTable row) throws Exception {
        String table = requireSafeIdentifier(row.getTableName());
        if (row.getTargetDataSourceId() == null) {
            try (Connection conn = orchestratorDataSource.getConnection();
                 Statement st = conn.createStatement()) {
                st.execute("DROP TABLE IF EXISTS " + table);
            }
            return;
        }
        Map<String, Object> dataSource = jdbc.queryForMap(
                "SELECT url, username, password FROM data_source WHERE id = ?", row.getTargetDataSourceId());
        try (Connection conn = DriverManager.getConnection((String) dataSource.get("url"),
                (String) dataSource.get("username"), (String) dataSource.get("password"));
             Statement st = conn.createStatement()) {
            st.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    private static String requireSafeIdentifier(String identifier) {
        if (identifier == null || !SAFE_IDENTIFIER.matcher(identifier).matches()) {
            throw new IllegalArgumentException("unsafe target table name: " + identifier);
        }
        return identifier;
    }

    private static Date cutoff(int retentionDays) {
        long days = Math.max(0, retentionDays);
        return new Date(System.currentTimeMillis() - days * 24L * 60 * 60 * 1000);
    }

    private static String abbreviate(String value, int max) {
        if (value == null) {
            return null;
        }
        return value.length() <= max ? value : value.substring(0, max);
    }

    /** 清理结果（API 直接序列化）。 */
    public record CleanupReport(boolean dryRun, int candidates, int dropped, List<String> details) {
    }
}
