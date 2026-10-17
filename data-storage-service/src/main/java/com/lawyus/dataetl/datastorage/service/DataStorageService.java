package com.lawyus.dataetl.datastorage.service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.datastorage.client.EtlTask;
import com.lawyus.dataetl.datastorage.client.TaskManagementServiceClient;

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

    @Value("${etl.topics.task-completed}")
    private String taskCompletedTopic;

    /** 消费端幂等去重缓存的过期时长 */
    private static final Duration DEDUP_TTL = Duration.ofHours(24);

    /** 入库失败最大重试次数 */
    private static final int INSERT_MAX_ATTEMPTS = 3;

    public void storeData(String transformedData) {
        if (transformedData == null || transformedData.isEmpty()) {
            log.warn("Received empty transformed data, skipping storage");
            return;
        }

        try {
            JsonNode rootNode = objectMapper.readTree(transformedData);
            String taskId = rootNode.get("taskId").asText();
            JsonNode dataNode = rootNode.get("data");

            // 修复：上游每条消息的 data 是单行对象（非数组），原 isArray() 校验会导致每条都抛异常、无法入库
            if (dataNode == null || !dataNode.isObject()) {
                throw new RuntimeException("Data object not found or invalid in transformed data for task: " + taskId);
            }

            // 消费端幂等去重：基于 taskId + 主键 id（auto-offset-reset=earliest 会在重启时重放）
            String dedupKey = buildDedupKey(taskId, dataNode);
            if (dedupKey != null && Boolean.TRUE.equals(redisTemplate.hasKey(dedupKey))) {
                log.info("Duplicate record skipped for task {}: key={}", taskId, dedupKey);
                return;
            }

            // 从任务管理服务获取目标表信息
            String tableName = getTargetTableNameFromTaskService(taskId);

            if (tableName == null || tableName.isEmpty()) {
                throw new RuntimeException("Failed to get target table name for task: " + taskId);
            }

            Map<String, Object> rowData = convertJsonArrayToList(dataNode);
            // 存储数据到数据库（目标表由任务管理服务预先创建），失败最多重试 INSERT_MAX_ATTEMPTS 次
            storeBatchDataToDatabase(tableName, rowData);

            // 入库成功后再打上去重标记；若中途异常则不标记，交由 Kafka 重试重新处理
            if (dedupKey != null) {
                redisTemplate.opsForValue().set(dedupKey, "1", DEDUP_TTL);
            }

            // 在数据存储成功后，更新 Redis hash 中的当前任务完成记录数
            updateCompletedRecords(taskId);

        } catch (Exception e) {
            log.error("Error storing data: {}", e.getMessage());
            throw new RuntimeException("Error storing transformed data", e);
        }
    }

    private String getTargetTableNameFromTaskService(String taskId) {
        try {
            EtlTask task =
                    taskManagementServiceClient.getTaskByTaskId(taskId);

            if (task != null) {
                return task.getTargetTableName();
            }

            return null;
        } catch (Exception e) {
            log.error("Error getting target table name from task management service for task {}: {}", taskId, e.getMessage());
            throw new RuntimeException("Error retrieving target table information", e);
        }
    }

    /**
     * 构建去重缓存 key：etl:dedup:{taskId}:{主键id}；无 id 字段时返回 null（不去重）
     */
    private String buildDedupKey(String taskId, JsonNode dataNode) {
        JsonNode idNode = dataNode.get("id");
        if (idNode == null || idNode.isNull()) {
            return null;
        }
        return "etl:dedup:" + taskId + ":" + idNode.asText();
    }

    /**
     * 存储数据到数据库，失败时指数退避重试，超过最大次数后抛出异常交由 Kafka 重试
     */
    private void storeBatchDataToDatabase(String tableName, Map<String, Object> data) {
        String insertSql = buildDynamicBatchInsertSqlWithGeometryHandling(tableName, data);
        Object[] values = new Object[data.size()];
        int valueIndex = 0;
        for (Object value : data.values()) {
            // 特殊处理日期字符串，转换为java.sql.Date
            if (value instanceof String str && str.matches("^\\d{4}-\\d{2}-\\d{2}$")) {
                values[valueIndex] = java.sql.Date.valueOf(str);
            } else {
                values[valueIndex] = value;
            }
            valueIndex++;
        }
        List<Object[]> batchArgs = new ArrayList<>();
        batchArgs.add(values);

        int attempt = 0;
        while (true) {
            try {
                attempt++;
                jdbcTemplate.batchUpdate(insertSql, batchArgs);
                return;
            } catch (Exception e) {
                if (attempt >= INSERT_MAX_ATTEMPTS) {
                    log.error("Insert into {} failed after {} attempts: {}", tableName, attempt, e.getMessage());
                    throw new RuntimeException("Failed to store data into " + tableName, e);
                }
                log.warn("Insert into {} failed (attempt {}/{}), retrying: {}", tableName, attempt, INSERT_MAX_ATTEMPTS, e.getMessage());
                try {
                    Thread.sleep(200L * attempt);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted while retrying insert into " + tableName, e);
                }
            }
        }
    }

    /**
     * 构建批量插入SQL（带几何字段处理）
     */
    private String buildDynamicBatchInsertSqlWithGeometryHandling(String tableName, Map<String, Object> data) {

        StringBuilder sql = new StringBuilder();
        StringBuilder columns = new StringBuilder();
        StringBuilder placeholders = new StringBuilder();

        sql.append("INSERT INTO ").append(tableName).append(" (");

        // 添加列名和占位符
        boolean first = true;
        for (Map.Entry<String, Object> entry : data.entrySet()) {
            String columnName = entry.getKey();

            if (!first) {
                columns.append(", ");
                placeholders.append(", ");
            }

            columns.append(columnName);

            // 特殊处理几何字段
            if (("geom".equalsIgnoreCase(columnName) || "geometry".equalsIgnoreCase(columnName)) && entry.getValue() != null) {
                String geometryValue = entry.getValue().toString();
                if (geometryValue.startsWith("0") || geometryValue.startsWith("1") || geometryValue.startsWith("2") ||
                        geometryValue.startsWith("3") || geometryValue.startsWith("4") || geometryValue.startsWith("5") ||
                        geometryValue.startsWith("6") || geometryValue.startsWith("7") || geometryValue.startsWith("8") ||
                        geometryValue.startsWith("9")) {
                    // WKB十六进制格式，使用ST_GeomFromWKB函数转换
                    placeholders.append("ST_GeomFromWKB(decode(?, 'hex'))");
                } else {
                    // WKT格式，使用ST_GeomFromText函数转换
                    placeholders.append("ST_GeomFromText(?)");
                }
            } else {
                placeholders.append("?");
            }
            first = false;
        }

        // 构建完整SQL
        sql.append(columns).append(") VALUES (").append(placeholders).append(")");

        return sql.toString();
    }

    /**
     * 将JsonNode数组转换为Map列表
     */
    private Map<String, Object> convertJsonArrayToList(JsonNode dataArray) {
        Map<String, Object> rowMap = new HashMap<>();
        Iterator<Map.Entry<String, JsonNode>> fields = dataArray.fields();

        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            String fieldName = field.getKey();
            JsonNode fieldValue = field.getValue();
            // 添加到行映射
            rowMap.put(fieldName, getValueForSql(fieldValue));
        }
        return rowMap;
    }

    /**
     * 获取SQL参数值
     */
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
            String textValue = value.asText();

            // Let JDBC handle date conversion automatically
            return textValue;
        } else {
            return value.toString();
        }
    }

    /**
     * 更新 Redis hash 中的当前任务已完成记录数
     * 判断是否等于任务总记录数，如果相等则发送任务完成消息
     */
    private void updateCompletedRecords(String taskId) {
        try {
            String redisKey = "task:status:" + taskId;
            String totalRecordsKey = "totalRecords";
            String completedRecordsKey = "completedRecords";

            long completedRecords = redisTemplate.opsForHash().increment(redisKey, completedRecordsKey, 1L);

            // 获取总记录数和当前已完成记录数
            Object totalRecordsObj = redisTemplate.opsForHash().get(redisKey, totalRecordsKey);
            if (totalRecordsObj == null) {
                log.warn("totalRecords not initialized in Redis for task {}, skip completion check", taskId);
                return;
            }
            long totalRecords = Long.parseLong(totalRecordsObj.toString());

            // 判断是否已达到任务总记录数
            if (completedRecords >= totalRecords) {
                sendTaskCompletedMessage(taskId, completedRecords, totalRecords);
                log.info("All records completed for task {}, sent task completed message", taskId);
            }

        } catch (Exception e) {
            log.error("Error updating completed records for task {}: {}", taskId, e.getMessage());
        }
    }

    /**
     * 发送任务完成消息
     */
    private void sendTaskCompletedMessage(String taskId, long completedRecords, long totalRecords) {
        try {
            // 构建任务完成消息
            Map<String, String> messageMap = new HashMap<>();
            messageMap.put("taskId", taskId);
            messageMap.put("status", "completed");
            messageMap.put("timestamp", String.valueOf(System.currentTimeMillis()));
            messageMap.put("completedRecords", String.valueOf(completedRecords));
            messageMap.put("totalRecords", String.valueOf(totalRecords));

            String message = objectMapper.writeValueAsString(messageMap);

            // 发送消息到Kafka
            kafkaTemplate.send(taskCompletedTopic, taskId, message);
        } catch (Exception e) {
            log.error("Error sending task completed message for task {}: {}", taskId, e.getMessage());
        }
    }
}