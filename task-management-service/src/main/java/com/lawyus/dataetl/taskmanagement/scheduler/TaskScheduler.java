package com.lawyus.dataetl.taskmanagement.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.client.DataSource;
import com.lawyus.dataetl.taskmanagement.client.DataSourceServiceClient;
import com.lawyus.dataetl.taskmanagement.entity.EtlTask;
import com.lawyus.dataetl.taskmanagement.entity.EtlTaskMessage;
import com.lawyus.dataetl.taskmanagement.repository.EtlTaskRepository;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;
import org.springframework.util.CollectionUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Component("etlTaskScheduler")
@Slf4j
public class TaskScheduler {

    @Autowired
    private EtlTaskRepository etlTaskRepository;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Value("${etl.topics.task-request}")
    private String taskRequestTopic;

    @Autowired
    private DataSourceServiceClient dataSourceServiceClient;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private RedissonClient redissonClient;

    private static final String TASK_LOCK_KEY ="task:scheduler:lock";

    @Scheduled(fixedRate = 30_000)
    public void processNewTasks() {
        RLock rLock = redissonClient.getLock(TASK_LOCK_KEY);
        List<EtlTask> newTasks;
        if (rLock.tryLock()) {
            try {
                newTasks = etlTaskRepository.findByStatus("new");
                if (CollectionUtils.isEmpty(newTasks)) return;
                newTasks.forEach(this::updateStatus);
            } finally {
                rLock.unlock();
            }
            newTasks.forEach(this::processNewTask);
        }
    }

    private void updateStatus(EtlTask task) {
        task.setStatus("processing");
        task.setStartTime(new Date());
        etlTaskRepository.save(task);
    }

    public void processNewTask(EtlTask task) {
        try {
            // 获取数据源信息
            DataSource dataSourceInfo = dataSourceServiceClient.getDataSourceById(task.getDataSourceId());
            // 创建目标表
            createTargetTable(dataSourceInfo, task.getSourceTableName(), task.getTargetTableName());

            EtlTaskMessage taskMessage = getEtlTaskMessage(task, dataSourceInfo);

            ObjectMapper om = new ObjectMapper();
            kafkaTemplate.send(taskRequestTopic, task.getTaskId(), om.writeValueAsString(taskMessage));

            task.setStatus("running");
            task.setStartTime(new Date());
            etlTaskRepository.save(task);
        } catch (Exception e) {
            log.error("Error processing task {}: {}", task.getTaskId(), e.getMessage());
            task.setStatus("failed");
            etlTaskRepository.save(task);
        }

    }

    private static EtlTaskMessage getEtlTaskMessage(EtlTask task, DataSource dataSourceInfo) {
        EtlTaskMessage taskMessage = new EtlTaskMessage();
        taskMessage.setTaskId(task.getTaskId());
        taskMessage.setSourceTableName(task.getSourceTableName());

        EtlTaskMessage.DataSource dataSource = new EtlTaskMessage.DataSource();
        BeanUtils.copyProperties(dataSourceInfo, dataSource);
        taskMessage.setDataSource(dataSource);
        return taskMessage;
    }

    /**
     * 创建目标表
     *
     * @param dataSourceInfo  数据源信息
     * @param sourceTableName 源表名
     * @param targetTableName 目标表名
     */
    private void createTargetTable(DataSource dataSourceInfo, String sourceTableName, String targetTableName) throws SQLException {
        // 1. 从源数据库获取源表结构
        List<ColumnDefinition> sourceColumns = getSourceTableColumns(dataSourceInfo, sourceTableName);

        // 2. 生成目标表创建SQL
        String createTableSql = generateCreateTableSql(targetTableName, sourceColumns);

        // 3. 在目标数据库中执行SQL创建表
        jdbcTemplate.execute(createTableSql);
        // 手动关闭资源
    }

    /**
     * 从源数据库获取源表结构
     *
     * @param dataSourceInfo  数据源信息
     * @param sourceTableName 源表名
     * @return 源表列定义列表
     */
    private List<ColumnDefinition> getSourceTableColumns(DataSource dataSourceInfo, String sourceTableName) throws SQLException {
        List<ColumnDefinition> columns = new ArrayList<>();

        try (Connection conn = getConnection(dataSourceInfo);
             ResultSet rs = conn.getMetaData()
                     .getColumns(null, null, sourceTableName, null);) {

            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                int dataType = rs.getInt("DATA_TYPE");
                String typeName = rs.getString("TYPE_NAME");
                int columnSize = rs.getInt("COLUMN_SIZE");
                String isNullable = rs.getString("IS_NULLABLE");
                String columnDef = rs.getString("COLUMN_DEF");

                columns.add(new ColumnDefinition(columnName, dataType, typeName, columnSize, isNullable, columnDef));
            }
        }
        return columns;
    }

    /**
     * 生成目标表创建SQL
     *
     * @param targetTableName 目标表名
     * @param sourceColumns   源表列定义列表
     * @return 目标表创建SQL
     */
    private String generateCreateTableSql(String targetTableName, List<ColumnDefinition> sourceColumns) {
        StringBuilder createTableSql = new StringBuilder();
        createTableSql.append("CREATE TABLE IF NOT EXISTS ").append(targetTableName).append(" (");

        // 添加源表列，保持与源表结构一致
        for (int i = 0; i < sourceColumns.size(); i++) {
            ColumnDefinition column = sourceColumns.get(i);
            String columnName = column.columnName();
            String columnType = column.typeName();
            String columnDef = column.columnDef();

            // 特殊处理geometry字段，确保完整的类型定义
            if (columnType.equalsIgnoreCase("geometry")) {
                // 对于geometry字段，使用完整的PostGIS类型定义
                createTableSql.append(columnName).append(" geometry(polygon, 4326)");
            } else {
                createTableSql.append(columnName).append(" ").append(columnType);

                // 添加列大小（如果适用）
                if (column.columnSize() > 0 &&
                        (columnType.equalsIgnoreCase("VARCHAR") ||
                                columnType.equalsIgnoreCase("CHAR") ||
                                columnType.equalsIgnoreCase("NVARCHAR") ||
                                columnType.equalsIgnoreCase("NCHAR"))) {
                    createTableSql.append("(").append(column.columnSize()).append(")");
                }
            }

            if ("NO".equalsIgnoreCase(column.isNullable())) {
                createTableSql.append(" NOT NULL");
            }

            // 添加默认值，但要避免serial类型字段出现多个默认值
            if (columnDef != null && !columnDef.isEmpty() &&
                    !columnType.equalsIgnoreCase("serial") &&
                    !columnType.equalsIgnoreCase("bigserial")) {
                createTableSql.append(" DEFAULT ").append(columnDef);
            }

            // 添加逗号分隔符
            if (i < sourceColumns.size() - 1) {
                createTableSql.append(", ");
            }
        }
        createTableSql.append(")");

        return createTableSql.toString();
    }

    private record ColumnDefinition
            (String columnName, int dataType, String typeName, int columnSize, String isNullable, String columnDef) {
    }

    private Connection getConnection(DataSource dataSourceInfo) throws SQLException {
        try {
            Class.forName(dataSourceInfo.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new RuntimeException("PostgreSQL JDBC driver not found", e);
        }

        String url = dataSourceInfo.getUrl();
        String username = dataSourceInfo.getUsername();
        String password = dataSourceInfo.getPassword();

        return DriverManager.getConnection(url, username, password);
    }
}