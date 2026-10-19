package com.lawyus.dataetl.worker.checkpoint;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.ArrayList;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class CheckpointWriterSqlTest {

    private CapturingJdbcTemplate jdbcTemplate;
    private CheckpointWriter writer;

    @BeforeEach
    void setUp() {
        jdbcTemplate = new CapturingJdbcTemplate();
        writer = new CheckpointWriter(jdbcTemplate);
    }

    @Test
    @DisplayName("UPSERT 带 shard_attempt fencing、终态保护与 filtered/failed 分列")
    void upsertContainsFencingAndNewColumns() {
        writer.upsert("task-1", "run-1", "shard-0", "RUNNING", "42", "42", 3,
                100, 90, 5, 5, 10, 7, null, null);

        assertThat(jdbcTemplate.sqls).hasSize(1);
        String sql = jdbcTemplate.sqls.get(0);
        assertThat(sql).contains("ON CONFLICT (task_id, run_id, shard_id) DO UPDATE")
                .contains("EXCLUDED.shard_attempt < etl_task_checkpoint.shard_attempt")
                .contains("etl_task_checkpoint.status IN ('SUCCEEDED','FAILED')")
                .contains("GREATEST(etl_task_checkpoint.rows_in, EXCLUDED.rows_in)")
                .contains("rows_filtered = GREATEST")
                .contains("rows_failed = GREATEST");

        Object[] values = jdbcTemplate.args.get(0);
        assertThat(values).hasSize(15);
        assertThat(values[0]).isEqualTo("task-1");
        assertThat(values[4]).isEqualTo(3);        // shard_attempt
        assertThat(values[5]).isEqualTo("42");     // last_cursor
        assertThat(values[7]).isEqualTo(100L);     // rows_in
        assertThat(values[9]).isEqualTo(5L);       // rows_filtered
        assertThat(values[10]).isEqualTo(5L);      // rows_failed
        assertThat(values[11]).isEqualTo(10L);     // rows_dropped
    }

    @Test
    @DisplayName("renewLease 仅在本 attempt 仍持有时更新")
    void renewLeaseIsFencedByAttempt() {
        writer.renewLease("task-1", "run-1", "shard-0", "worker-a", 2,
                new java.sql.Timestamp(System.currentTimeMillis() + 60_000));

        assertThat(jdbcTemplate.sqls).hasSize(1);
        assertThat(jdbcTemplate.sqls.get(0)).contains("shard_attempt = ?");
        assertThat(jdbcTemplate.args.get(0)).containsSequence("task-1", "run-1", "shard-0", 2);
    }

    /** 不触库：只截获 SQL/参数，验证 SQL 契约。 */
    private static final class CapturingJdbcTemplate extends JdbcTemplate {
        private final List<String> sqls = new ArrayList<>();
        private final List<Object[]> args = new ArrayList<>();

        @Override
        public int update(String sql, Object... args) {
            sqls.add(sql);
            this.args.add(args);
            return 1;
        }
    }
}
