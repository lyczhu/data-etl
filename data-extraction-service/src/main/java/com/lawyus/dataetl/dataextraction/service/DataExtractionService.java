package com.lawyus.dataetl.dataextraction.service;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.dataextraction.datasource.DynamicDataSource;
import com.lawyus.dataetl.dataextraction.entity.EtlTaskMessage;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DataExtractionService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private RedisTemplate<String, String> redisTemplate;

    @Autowired
    private DynamicDataSource dynamicDataSource;

    @Value("${etl.topics.data-extraction}")
    private String dataExtractionTopic;

    @Value("${data.extract.batch-size}")
    private int batchSize;

    @Value("${etl.legacy.redis-status-ttl-hours:24}")
    private long redisStatusTtlHours;

    public void processTask(EtlTaskMessage taskRequest) {
        EtlTaskMessage.DataSource dataSource = taskRequest.getDataSource();
        String taskId = taskRequest.getTaskId();
        String sourceTableName = taskRequest.getSourceTableName();

        // 使用动态数据源管理器获取主键范围
        long[] primaryKeyRange = getPrimaryKeyRange(dataSource, sourceTableName);
        long minId = primaryKeyRange[0];
        long maxId = primaryKeyRange[1];
        long totalRecords = primaryKeyRange[2];

        // 将总数添加到Redis hash中
        initializeRedisShardCounter(taskId, totalRecords);

        getAndSendData(minId, maxId, totalRecords, dataSource, sourceTableName, taskId);
    }

    private long[] getPrimaryKeyRange(EtlTaskMessage.DataSource dataSource, String tableName) {
        long minId = 0, maxId = 0, total = 0;

        try (Connection conn = dynamicDataSource.getConnection(dataSource.getId(), dataSource.getUrl(),
                dataSource.getUsername(), dataSource.getPassword(), dataSource.getDriverClassName());
             PreparedStatement stmt = conn.prepareStatement(
                     "SELECT MIN(id) as min_id, MAX(id) as max_id, COUNT(*) as total_records FROM " + tableName)) {

            ResultSet rs = stmt.executeQuery();
            if (rs.next()) {
                minId = rs.getLong("min_id");
                maxId = rs.getLong("max_id");
                total = rs.getInt("total_records");
            }

            log.info("Table {}: Primary key range is {}-{}", tableName, minId, maxId);

        } catch (SQLException e) {
            log.error("Error getting primary key range for table {}: {}", tableName, e.getMessage());
            throw new RuntimeException("Error getting primary key range", e);
        }

        return new long[]{minId, maxId, total};
    }

    private void getAndSendData(long start, long end, long totalRecords, EtlTaskMessage.DataSource dataSource, String tableName, String originalTaskId) {

        try (Connection conn = dynamicDataSource.getConnection(dataSource.getId(), dataSource.getUrl(),
                dataSource.getUsername(), dataSource.getPassword(), dataSource.getDriverClassName())) {
            // 1. 获取源表结构，生成动态查询SQL
            String selectSql = generateDynamicSelectSql(conn, tableName);
            String fullSql = selectSql + " WHERE id >= ? LIMIT ?";

            long startId = start;
            long completed = 0;
            do {
                try (PreparedStatement stmt = conn.prepareStatement(fullSql)) {
                    stmt.setLong(1, startId);
                    stmt.setLong(2, batchSize);

                    ResultSet rs = stmt.executeQuery();

                    while (rs.next()) {
                        Map<String, Object> rowData = buildRowDataAsMap(rs);

                        // 实际只需要更新一次，取最后一条数据 id
                        startId = rs.getLong("id");

                        Map<String, Object> data = new HashMap<>();
                        data.put("data", rowData);
                        data.put("taskId", originalTaskId);

                        sendBatchToKafka(data, originalTaskId);
                    }
                }
                startId++;
                completed += batchSize;
            } while (completed < totalRecords);

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * 根据表结构生成动态查询SQL
     *
     * @param conn      数据库连接
     * @param tableName 表名
     * @return 动态生成的SELECT SQL
     * @throws SQLException 数据库异常
     */
    private String generateDynamicSelectSql(Connection conn, String tableName) throws SQLException {
        StringBuilder selectSql = new StringBuilder("SELECT ");

        // 获取表的所有列
        DatabaseMetaData metaData = conn.getMetaData();
        try (ResultSet columns = metaData.getColumns(null, null, tableName, null)) {
            boolean firstColumn = true;
            while (columns.next()) {
                String columnName = columns.getString("COLUMN_NAME");
                String columnType = columns.getString("TYPE_NAME");

                if (!firstColumn) {
                    selectSql.append(", ");
                }

                // 特殊处理geometry字段，使用ST_AsText()转换为WKT格式
                if (columnType.equalsIgnoreCase("geometry")) {
                    selectSql.append("ST_AsText(").append(columnName).append(") as ").append(columnName);
                } else {
                    selectSql.append(columnName);
                }

                firstColumn = false;
            }
        }

        selectSql.append(" FROM ").append(tableName);
        return selectSql.toString();
    }

    private Map<String, Object> buildRowDataAsMap(ResultSet rs) throws SQLException {
        // 动态根据表结构构建Map对象
        Map<String, Object> rowMap = new HashMap<>();

        // 获取结果集元数据，动态获取列名
        java.sql.ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnName(i);
            int columnType = metaData.getColumnType(i);
            Object value = rs.getObject(i);

            if (value != null) {
                // 处理不同数据类型的值
                Object processedValue = processColumnValue(columnType, value);
                rowMap.put(columnName, processedValue);
            } else {
                rowMap.put(columnName, null);
            }
        }

        return rowMap;
    }

    private Object processColumnValue(int columnType, Object value) {
        if (value == null) {
            return null;
        }

        String stringValue = value.toString();

        // 检查是否为数字类型
        try {
            if (columnType == java.sql.Types.INTEGER ||
                    columnType == java.sql.Types.BIGINT ||
                    columnType == java.sql.Types.DOUBLE ||
                    columnType == java.sql.Types.FLOAT ||
                    columnType == java.sql.Types.DECIMAL ||
                    columnType == java.sql.Types.NUMERIC) {
                // 尝试解析数值类型
                if (columnType == java.sql.Types.INTEGER) {
                    return Integer.valueOf(stringValue);
                } else if (columnType == java.sql.Types.BIGINT) {
                    return Long.valueOf(stringValue);
                } else if (columnType == java.sql.Types.DECIMAL || columnType == java.sql.Types.NUMERIC) {
                    return new java.math.BigDecimal(stringValue);
                } else {
                    return Double.valueOf(stringValue);
                }
            }
        } catch (NumberFormatException e) {
            // 如果无法解析为数字，则作为字符串处理
        }
        // 对于其他类型（如VARCHAR, TEXT等）或解析失败的情况，返回字符串
        return stringValue;
    }

    private void sendBatchToKafka(Map<String, Object> datas, String taskId) {
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            String json = objectMapper.writeValueAsString(datas);

            kafkaTemplate.send(dataExtractionTopic, taskId, json);
        } catch (Exception e) {
            log.error("Task {}: Error sending batch to Kafka: {}", taskId, e.getMessage());
            throw new RuntimeException("Error sending data to Kafka", e);
        }
    }

    // 初始化Redis分片计数器
    private void initializeRedisShardCounter(String taskId, long totalRecords) {
        String redisKey = "task:status:" + taskId;
        Map<String, String> recordsInfo = new HashMap<>();
        recordsInfo.put("totalRecords", String.valueOf(totalRecords));
        recordsInfo.put("completedRecords", "0");

        redisTemplate.opsForHash().putAll(redisKey, recordsInfo);
        // 旧链路状态键是展示缓存，不是 completion 真源；没有 TTL 会在 Redis 中永久堆积。
        redisTemplate.expire(redisKey, redisStatusTtlHours, TimeUnit.HOURS);
        log.info("Initialized Redis shard counter for task {}: totalRecords={}, completedRecords=0, ttlHours={}",
                taskId, totalRecords, redisStatusTtlHours);
    }
}