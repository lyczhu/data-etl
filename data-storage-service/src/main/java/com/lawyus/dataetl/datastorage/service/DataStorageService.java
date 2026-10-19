package com.lawyus.dataetl.datastorage.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.lawyus.dataetl.datastorage.client.EtlTask;
import com.lawyus.dataetl.datastorage.client.TaskManagementServiceClient;
import com.lawyus.dataetl.datastorage.config.EtlMetrics;

/**
 * 数据存储服务。
 *
 * Phase 0 性能修复（见 evolution-plan §7 Phase 0）：
 * 1) 目标表名 Caffeine 本地缓存，消除"每行一次 Feign 调用"；
 * 2) 批消费（batch listener）下真批累积 executeBatch + 单事务提交，消除"每行一次网络往返"；
 * 3) 完成计数按批增量 HINCRBY，减少对 Redis 的逐行写。
 */
@Service
public class DataStorageService {
    private static final Logger log = LoggerFactory.getLogger(DataStorageService.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private TaskManagementServiceClient taskManagementServiceClient;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private PlatformTransactionManager txManager;

    @Autowired
    private EtlMetrics etlMetrics;

    @Value("${etl.topics.task-completed}")
    private String taskCompletedTopic;

    /** 消费端幂等去重缓存的过期时长 */
    private static final Duration DEDUP_TTL = Duration.ofHours(24);

    /** 入库失败最大重试次数 */
    private static final int INSERT_MAX_ATTEMPTS = 3;

    /** taskId -> 目标表名 本地缓存（TTL 5min），消除逐行 Feign */
    private final Cache<String, String> targetTableCache = Caffeine.newBuilder()
            .maximumSize(1000)
            .expireAfterWrite(Duration.ofMinutes(5))
            .build();

    private TransactionTemplate txTemplate;

    @PostConstruct
    void init() {
        this.txTemplate = new TransactionTemplate(txManager);
    }

    /**
     * 单条入口（保留兼容）：委托批处理。
     */
    public void storeData(String transformedData) {
        storeBatch(Collections.singletonList(transformedData));
    }

    /**
     * 批量消费入口：解析 → 去重 → 按 taskId 分组 → 真批入库（单事务）→ 打去重标记 → 批量递增完成计数。
     */
    public void storeBatch(List<String> messages) {
        if (messages == null || messages.isEmpty()) {
            return;
        }

        // 按 taskId 聚合待入库行；malformed 消息记录并跳过（避免毒消息令整批无限重试）
        Map<String, List<PendingRow>> rowsByTask = new LinkedHashMap<>();
        for (String message : messages) {
            if (message == null || message.isEmpty()) {
                continue;
            }
            String taskId;
            JsonNode dataNode;
            try {
                JsonNode rootNode = objectMapper.readTree(message);
                taskId = rootNode.path("taskId").asText(null);
                dataNode = rootNode.path("data");
                if (taskId == null || taskId.isEmpty()) {
                    log.warn("Skip message without taskId: {}", message);
                    continue;
                }
            } catch (Exception e) {
                log.warn("Skip malformed transformed message (poison guard): {}", e.getMessage());
                etlMetrics.recordDropped();
                continue;
            }

            // 修复：上游每条消息的 data 是单行对象（非数组）
            if (dataNode == null || !dataNode.isObject()) {
                log.warn("Skip task {} message with invalid data node", taskId);
                continue;
            }

            // 消费端幂等去重：基于 taskId + 主键 id
            String dedupKey = buildDedupKey(taskId, dataNode);
            if (dedupKey != null && Boolean.TRUE.equals(redisTemplate.hasKey(dedupKey))) {
                log.debug("Duplicate record skipped for task {}: key={}", taskId, dedupKey);
                etlMetrics.recordDeduped();
                continue;
            }

            etlMetrics.recordIn(1);
            Map<String, Object> row = convertJsonNodeToRowMap(dataNode);
            rowsByTask.computeIfAbsent(taskId, k -> new ArrayList<>())
                    .add(new PendingRow(dedupKey, row));
        }

        for (Map.Entry<String, List<PendingRow>> entry : rowsByTask.entrySet()) {
            List<PendingRow> rows = entry.getValue();
            if (rows.isEmpty()) {
                continue;
            }
            storeTaskRows(entry.getKey(), rows);
        }
    }

    /**
     * 将一个任务的若干行真批入库到目标表（单事务，all-or-nothing），成功后打去重标记并按批递增完成数。
     */
    private void storeTaskRows(String taskId, List<PendingRow> rows) {
        MDC.put("taskId", taskId);
        long startNanos = System.nanoTime();
        try {
            String tableName = getCachedTargetTableName(taskId);
            if (tableName == null || tableName.isEmpty()) {
                throw new RuntimeException("Failed to get target table name for task: " + taskId);
            }

            List<String> columns = new ArrayList<>(rows.get(0).row.keySet());
            String insertSql = buildInsertSqlFromColumns(tableName, columns, rows.get(0).row);

            List<Object[]> batchArgs = new ArrayList<>(rows.size());
            for (PendingRow pendingRow : rows) {
                batchArgs.add(toSqlArgs(pendingRow.row, columns));
            }

            // 分批提交：单批过大时切片，避免一次 executeBatch 事务过重
            int written = 0;
            int chunk = 500;
            for (int from = 0; from < batchArgs.size(); from += chunk) {
                int to = Math.min(from + chunk, batchArgs.size());
                List<Object[]> sub = batchArgs.subList(from, to);
                executeBatchWithRetry(tableName, insertSql, sub);
                written += sub.size();
            }

            // 入库成功后再打去重标记；单事务 all-or-nothing，异常时不提交也不标记
            for (PendingRow pendingRow : rows) {
                if (pendingRow.dedupKey != null) {
                    redisTemplate.opsForValue().set(pendingRow.dedupKey, "1", DEDUP_TTL);
                }
            }

            etlMetrics.recordOut(written);
            incrementCompletedRecords(taskId, written);
        } finally {
            etlMetrics.recordLoadDuration(Duration.ofNanos(System.nanoTime() - startNanos));
            MDC.remove("taskId");
        }
    }

    /**
     * 在独立事务中执行一批 INSERT，失败时指数退避重试；超过最大次数后抛出，交由 Kafka 重投整批。
     */
    private void executeBatchWithRetry(String tableName, String insertSql, List<Object[]> batchArgs) {
        int attempt = 0;
        while (true) {
            try {
                attempt++;
                txTemplate.executeWithoutResult(status -> jdbcTemplate.batchUpdate(insertSql, batchArgs));
                return;
            } catch (Exception e) {
                if (attempt >= INSERT_MAX_ATTEMPTS) {
                    log.error("Batch insert into {} failed after {} attempts: {}", tableName, attempt, e.getMessage());
                    throw new RuntimeException("Failed to store " + batchArgs.size() + " rows into " + tableName, e);
                }
                log.warn("Batch insert into {} failed (attempt {}/{}), retrying: {}",
                        tableName, attempt, INSERT_MAX_ATTEMPTS, e.getMessage());
                etlMetrics.recordInsertRetry();
                try {
                    Thread.sleep(200L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while retrying batch insert into " + tableName, e);
                }
            }
        }
    }

    /**
     * 带 Caffeine 缓存的目标表名获取（读穿透）。
     */
    private String getCachedTargetTableName(String taskId) {
        return targetTableCache.get(taskId, id -> {
            try {
                EtlTask task = taskManagementServiceClient.getTaskByTaskId(id);
                return task != null ? task.getTargetTableName() : null;
            } catch (Exception e) {
                log.error("Error getting target table name for task {}: {}", id, e.getMessage());
                throw new RuntimeException("Error retrieving target table information", e);
            }
        });
    }

    /**
     * 构建去重缓存 key：etl:dedup:{taskId}:{主键id}；无 id 字段时返回 null（不去重）。
     */
    private String buildDedupKey(String taskId, JsonNode dataNode) {
        JsonNode idNode = dataNode.get("id");
        if (idNode == null || idNode.isNull()) {
            return null;
        }
        return "etl:dedup:" + taskId + ":" + idNode.asText();
    }

    /**
     * 根据列顺序 + 首行样本（用于几何列 WKT/WKB 判别）构建带几何处理的批量 INSERT SQL。
     * 同一任务的所有行来自同一源表，列集合/顺序与几何格式一致。
     */
    private String buildInsertSqlFromColumns(String tableName, List<String> columns, Map<String, Object> sampleRow) {
        StringBuilder sql = new StringBuilder("INSERT INTO ").append(tableName).append(" (");
        StringBuilder placeholders = new StringBuilder();
        boolean first = true;
        for (String column : columns) {
            if (!first) {
                sql.append(", ");
                placeholders.append(", ");
            }
            sql.append(column);
            Object value = sampleRow.get(column);
            if (isGeometryColumn(column) && value != null) {
                String geometryValue = value.toString();
                if (looksLikeWkbHex(geometryValue)) {
                    placeholders.append("ST_GeomFromWKB(decode(?, 'hex'))");
                } else {
                    placeholders.append("ST_GeomFromText(?)");
                }
            } else {
                placeholders.append("?");
            }
            first = false;
        }
        sql.append(") VALUES (").append(placeholders).append(")");
        return sql.toString();
    }

    /**
     * 按列顺序将行 Map 转为 SQL 参数数组，并将 yyyy-MM-dd 字符串转为 java.sql.Date。
     */
    private Object[] toSqlArgs(Map<String, Object> row, List<String> columns) {
        Object[] args = new Object[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            Object value = row.get(columns.get(i));
            if (value instanceof String str && str.matches("^\\d{4}-\\d{2}-\\d{2}$")) {
                args[i] = java.sql.Date.valueOf(str);
            } else {
                args[i] = value;
            }
        }
        return args;
    }

    private boolean isGeometryColumn(String columnName) {
        return "geom".equalsIgnoreCase(columnName) || "geometry".equalsIgnoreCase(columnName);
    }

    private boolean looksLikeWkbHex(String geometryValue) {
        char c0 = geometryValue.charAt(0);
        return c0 >= '0' && c0 <= '9';
    }

    /**
     * 将 JsonNode（单行对象）转换为保持字段顺序的 Map（LinkedHashMap 保证跨行一致，供批量绑定使用）。
     */
    private Map<String, Object> convertJsonNodeToRowMap(JsonNode dataNode) {
        Map<String, Object> rowMap = new LinkedHashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = dataNode.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            rowMap.put(field.getKey(), getValueForSql(field.getValue()));
        }
        return rowMap;
    }

    private Object getValueForSql(JsonNode value) {
        if (value.isNull()) {
            return null;
        } else if (value.isInt()) {
            return value.asInt();
        } else if (value.isLong()) {
            return value.asLong();
        } else if (value.isDouble() || value.isFloat()) {
            return value.asDouble();
        } else if (value.isBoolean()) {
            return value.asBoolean();
        } else if (value.isTextual()) {
            return value.asText();
        } else {
            return value.toString();
        }
    }

    /**
     * 按批增量更新 Redis hash 中的完成记录数，达到总数时发送任务完成消息。
     */
    private void incrementCompletedRecords(String taskId, long delta) {
        try {
            String redisKey = "task:status:" + taskId;
            String totalRecordsKey = "totalRecords";
            String completedRecordsKey = "completedRecords";

            long completedRecords = redisTemplate.opsForHash().increment(redisKey, completedRecordsKey, delta);

            Object totalRecordsObj = redisTemplate.opsForHash().get(redisKey, totalRecordsKey);
            if (totalRecordsObj == null) {
                log.warn("totalRecords not initialized in Redis for task {}, skip completion check", taskId);
                return;
            }
            long totalRecords = Long.parseLong(totalRecordsObj.toString());

            if (completedRecords >= totalRecords) {
                sendTaskCompletedMessage(taskId, completedRecords, totalRecords);
                log.info("All records completed for task {}, sent task completed message", taskId);
            }
        } catch (Exception e) {
            log.error("Error updating completed records for task {}: {}", taskId, e.getMessage());
        }
    }

    private void sendTaskCompletedMessage(String taskId, long completedRecords, long totalRecords) {
        try {
            Map<String, String> messageMap = new LinkedHashMap<>();
            messageMap.put("taskId", taskId);
            messageMap.put("status", "completed");
            messageMap.put("timestamp", String.valueOf(System.currentTimeMillis()));
            messageMap.put("completedRecords", String.valueOf(completedRecords));
            messageMap.put("totalRecords", String.valueOf(totalRecords));

            String message = objectMapper.writeValueAsString(messageMap);
            kafkaTemplate.send(taskCompletedTopic, taskId, message);
        } catch (Exception e) {
            log.error("Error sending task completed message for task {}: {}", taskId, e.getMessage());
        }
    }

    /** 一行的待入库数据（去重键 + 行值 Map） */
    private static final class PendingRow {
        private final String dedupKey;
        private final Map<String, Object> row;

        private PendingRow(String dedupKey, Map<String, Object> row) {
            this.dedupKey = dedupKey;
            this.row = row;
        }
    }
}
