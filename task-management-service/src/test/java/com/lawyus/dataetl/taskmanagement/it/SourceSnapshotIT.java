package com.lawyus.dataetl.taskmanagement.it;

import com.lawyus.dataetl.connector.spi.CloseableIterator;
import com.lawyus.dataetl.connector.spi.ConnectorType;
import com.lawyus.dataetl.connector.spi.jdbc.JdbcSourceConnector;
import com.lawyus.dataetl.connector.spi.model.Shard;
import com.lawyus.dataetl.connector.spi.model.ShardParams;
import com.lawyus.dataetl.connector.spi.model.ShardPlan;
import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.connector.spi.model.SourceSpec;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * WP-17：源端快照语义。分片计划在 V-2 生成时冻结 {@code pkTo}（主键高水位）；
 * run 期间新插入的行（pk > 高水位）不进入本次 run，保证“本次 run 的行数 = 计划时快照行数”。
 */
class SourceSnapshotIT extends AbstractOrchestratorIT {

    @Test
    @DisplayName("计划生成后源表新增行：已冻结的分片区间不读取新行（快照高水位）")
    void planFreezesHighWatermark() {
        String table = "it_source_snapshot_" + UUID.randomUUID().toString().substring(0, 8);
        JdbcSourceConnector connector = new JdbcSourceConnector(ConnectorType.POSTGRES);
        SourceSpec spec = new SourceSpec(ConnectorType.POSTGRES, ItInfra.SOURCE_URL,
                ItInfra.PG_USER, ItInfra.PG_PASSWORD, ItInfra.SOURCE_DRIVER, table, 30);
        try {
            jdbc.execute("DROP TABLE IF EXISTS " + table);
            jdbc.execute("CREATE TABLE " + table + " (id BIGINT PRIMARY KEY, name VARCHAR(64))");
            for (long i = 1; i <= 20; i++) {
                jdbc.update("INSERT INTO " + table + " (id, name) VALUES (?, ?)", i, "snapshot-" + i);
            }

            ShardPlan plan = connector.planShards(spec, new ShardParams(8, 5000, 4));
            assertThat(plan.estimatedRows()).isEqualTo(20);
            assertThat(plan.shards()).hasSize(4);
            assertThat(plan.shards().stream().mapToLong(s -> s.pkTo() == null ? 0 : s.pkTo()).max().orElse(0))
                    .as("计划冻结的高水位 = 20").isEqualTo(20);

            // run 启动后源表继续写入：新行不在已冻结的 [pkFrom, pkTo] 内
            jdbc.update("INSERT INTO " + table + " (id, name) VALUES (21, 'late-row')");

            List<Long> readPks = new ArrayList<>();
            for (Shard shard : plan.shards()) {
                try (CloseableIterator<SourceRecord> rows = connector.open(shard, spec)) {
                    while (rows.hasNext()) {
                        readPks.add(((Number) rows.next().pk()).longValue());
                    }
                }
            }
            assertThat(readPks).hasSize(20).doesNotContain(21L);
        } catch (Exception e) {
            throw new IllegalStateException("snapshot IT failed", e);
        } finally {
            jdbc.execute("DROP TABLE IF EXISTS " + table);
        }
    }
}
