package com.lawyus.dataetl.taskmanagement.it;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Phase 2B：`ddl-auto=validate` 的启动自证。
 *
 * <p>本类能启动上下文，即说明 4 个 JPA 实体与 `db/schema`（02/05/06/07/08）完全一致；
 * 额外断言 02 补齐的两列与 08 的审计表确实存在，防止“Hibernate 静默跳过校验”。
 */
class SchemaValidationIT extends AbstractOrchestratorIT {

    @Test
    @DisplayName("validate 启动通过；etl_task 旧计数列与 etl_task_audit 均存在")
    void schemaValidatesAndRequiredColumnsExist() {
        assertThat(jdbc.queryForObject("SELECT to_regclass('public.etl_task_audit')", String.class))
                .isNotNull();
        Integer legacyColumns = jdbc.queryForObject(
                "SELECT COUNT(*) FROM information_schema.columns "
                        + "WHERE table_schema = 'public' AND table_name = 'etl_task' "
                        + "AND column_name IN ('total_records','completed_records')",
                Integer.class);
        assertThat(legacyColumns).isEqualTo(2);
    }
}
