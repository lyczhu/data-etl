-- ============================================================================
-- 00-extensions.sql
-- Phase 2A：PostGIS 扩展（MySQL→PG / PostGIS 真库回归的前置）。
-- docker-compose 的 postgres 镜像为 postgis/postgis:17-3.6-alpine，扩展已随镜像安装；
-- 本脚本只负责在目标库中启用。幂等可重复执行。
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS postgis;
