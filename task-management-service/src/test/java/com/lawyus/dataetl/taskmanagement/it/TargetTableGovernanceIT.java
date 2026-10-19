package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.connector.spi.model.ColumnDef;
import com.lawyus.dataetl.connector.spi.model.TableSchema;
import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.orchestrator.validate.TargetTableManager;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;

import java.sql.Types;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * WP-13 / Gate 2：V-3 目标表治理。
 *
 * <p>新表由 orchestrator 一次性建出（含 idempotency_key 主键）；存量表缺列时补列 + 唯一索引。
 * 唯一键冲突必须由数据库拒绝，不能靠 worker 每分片 CREATE TABLE 兜底。
 */
class TargetTableGovernanceIT extends AbstractOrchestratorIT {

    @Autowired
    private TargetTableManager targetTableManager;

    private static TableSchema schema(String table) {
        return new TableSchema(table, List.of(
                new ColumnDef("id", "int8", Types.BIGINT, 0, false),
                new ColumnDef("name", "varchar", Types.VARCHAR, 64, true)));
    }

    private static EtlTask task(String table) {
        EtlTask task = new EtlTask();
        task.setTaskId("it-" + UUID.randomUUID());
        task.setTargetTableName(table);
        return task;
    }

    @Test
    @DisplayName("新目标表：V-3 建表并带 idempotency_key 主键，worker 不再建表")
    void createsNewTableWithIdempotencyKey() {
        String table = "it_target_gov_" + UUID.randomUUID().toString().substring(0, 8);
        try {
            targetTableManager.ensure(task(table), schema(table));

            assertThat(jdbc.queryForObject("SELECT to_regclass(?)", String.class, table)).isNotNull();
            assertThat(columnCount(table, "idempotency_key")).isEqualTo(1);
            assertThat(uniqueIndexCount(table)).isGreaterThanOrEqualTo(1);
        } finally {
            jdbc.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    @Test
    @DisplayName("存量目标表缺幂等键：V-3 补列 + 唯一索引，重复键被数据库拒绝")
    void migratesExistingTableAndEnforcesUniqueKey() {
        String table = "it_target_gov_old_" + UUID.randomUUID().toString().substring(0, 8);
        try {
            jdbc.execute("CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, name VARCHAR(64))");

            targetTableManager.ensure(task(table), schema(table));

            assertThat(columnCount(table, "idempotency_key")).isEqualTo(1);
            assertThat(uniqueIndexCount(table)).isGreaterThanOrEqualTo(1);
            jdbc.update("INSERT INTO " + table + " (id, name, idempotency_key) VALUES (1, 'a', 'same-key')");
            assertThatThrownBy(() -> jdbc.update(
                    "INSERT INTO " + table + " (id, name, idempotency_key) VALUES (2, 'b', 'same-key')"))
                    .as("重复幂等键必须被唯一索引拒绝")
                    .isInstanceOf(DataAccessException.class);
        } finally {
            jdbc.execute("DROP TABLE IF EXISTS " + table);
        }
    }

    private int columnCount(String table, String column) {
        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM information_schema.columns "
                + "WHERE table_schema = current_schema() AND table_name = ? AND column_name = ?",
                Integer.class, table, column);
        return n == null ? 0 : n;
    }

    private int uniqueIndexCount(String table) {
        Integer n = jdbc.queryForObject("SELECT COUNT(*) FROM pg_indexes "
                + "WHERE schemaname = current_schema() AND tablename = ? "
                + "AND indexdef ILIKE '%idempotency_key%' AND indexdef ILIKE '%UNIQUE%'",
                Integer.class, table);
        return n == null ? 0 : n;
    }
}
