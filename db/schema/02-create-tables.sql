-- ETL系统核心表结构

-- 1. 数据源管理表
CREATE TABLE IF NOT EXISTS data_source (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL, -- 数据源名称
    type VARCHAR(50) NOT NULL, -- 数据源类型，如postgresql, mysql, oracle等
    url VARCHAR(255) NOT NULL, -- 数据库连接URL
    username VARCHAR(100) NOT NULL, -- 数据库用户名
    password VARCHAR(255) NOT NULL, -- 数据库密码，建议加密存储
    driver_class_name VARCHAR(255), -- 数据库驱动类名
    description TEXT, -- 数据源描述
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 创建时间
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP -- 更新时间
);

-- 2. ETL任务表
CREATE TABLE IF NOT EXISTS etl_task (
    id SERIAL PRIMARY KEY,
    task_id VARCHAR(64) NOT NULL UNIQUE, -- 任务UUID，全局唯一
    data_source_id INT NOT NULL REFERENCES data_source(id) ON DELETE CASCADE, -- 关联的数据源ID，删除数据源时级联删除相关任务
    target_data_source_id INT, -- 目标数据源ID（WP-03/D15；为空 = 写 worker 本地库）
    source_table_name VARCHAR(100) NOT NULL, -- 源表名称
    target_table_name VARCHAR(100) NOT NULL, -- 目标表名称
    status VARCHAR(20) NOT NULL DEFAULT 'new', -- 任务状态：new, running, completed, failed, paused
    total_shards INT NOT NULL DEFAULT 0, -- 总分片数
    completed_shards INT NOT NULL DEFAULT 0, -- 已完成分片数
    description VARCHAR(255), -- 任务描述
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 创建时间
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP, -- 更新时间
    start_time TIMESTAMP, -- 任务开始时间
    end_time TIMESTAMP, -- 任务结束时间
    error_message TEXT -- 错误信息，仅在任务失败时填充
);

-- 既有库迁移（幂等）
ALTER TABLE etl_task ADD COLUMN IF NOT EXISTS target_data_source_id INT;

-- 索引创建
CREATE INDEX IF NOT EXISTS idx_etl_task_status ON etl_task(status);
CREATE INDEX IF NOT EXISTS idx_etl_task_task_id ON etl_task(task_id);
CREATE INDEX IF NOT EXISTS idx_etl_task_data_source ON etl_task(data_source_id);
