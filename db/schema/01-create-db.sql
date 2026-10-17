-- 创建专用数据库
CREATE DATABASE etl_system
    WITH OWNER = postgres
    ENCODING = 'UTF8'
    LC_COLLATE = 'zh_CN.UTF-8'
    LC_CTYPE = 'zh_CN.UTF-8'
    TEMPLATE = template0;

-- 连接到新数据库（Docker 初始化脚本不支持 \c，改用以下方式）
\connect etl_system;

-- 创建普通用户（避免应用直连 superuser）
CREATE USER gis_user WITH PASSWORD 'gis_secure_pass';
ALTER USER gis_user CREATEDB;

-- 启用 PostGIS 扩展
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;

-- 授权
GRANT ALL PRIVILEGES ON DATABASE etl_system TO gis_user;
GRANT ALL ON SCHEMA public TO gis_user;