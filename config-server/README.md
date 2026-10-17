# 配置中心使用说明

## 概述

本项目已集成Spring Cloud Config配置中心，所有微服务的配置统一由配置中心管理。

## 配置中心服务

- **服务名称**: config-server
- **端口**: 8050
- **认证信息**:
  - 用户名: `config`
  - 密码: `config123`

## 配置文件存储

配置文件采用本地文件方式存储，位置在:
```
config-server/src/main/resources/config-repo/
```

各服务的配置文件:
- `data-source-service.yml` - 数据源服务配置
- `task-management-service.yml` - 任务管理服务配置
- `data-extraction-service.yml` - 数据提取服务配置
- `data-transformation-service.yml` - 数据转换服务配置
- `data-storage-service.yml` - 数据存储服务配置

## 客户端配置方式

各微服务使用 `spring.config.import` 方式连接配置中心，无需创建 `bootstrap.yml` 文件。

### 配置示例

```yaml
spring:
  application:
    name: data-source-service
  config:
    import: optional:configserver:http://localhost:8050
    username: config
    password: config123
```

### 配置说明

- `spring.application.name`: 服务名称，用于从配置中心获取对应的配置文件
- `spring.config.import`: 配置中心导入地址，使用 `optional:` 前缀表示配置中心不可用时服务仍可启动
- `spring.config.username` 和 `spring.config.password`: 配置中心认证信息

## 启动顺序

1. 启动基础设施服务 (PostgreSQL, Redis, Kafka)
2. 启动注册中心 (Registry Service)
3. 启动配置中心 (Config Server)
4. 启动其他微服务

## 配置中心API

### 获取配置

```bash
# 获取默认配置
curl -u config:config123 http://localhost:8050/{service-name}/default

# 获取特定profile的配置
curl -u config:config123 http://localhost:8050/{service-name}/{profile}
```

示例:
```bash
curl -u config:config123 http://localhost:8050/data-source-service/default
```

### 刷新配置

当配置文件修改后，需要刷新客户端配置:

```bash
# 刷新指定服务的配置
curl -X POST http://localhost:{port}/actuator/refresh
```

示例:
```bash
curl -X POST http://localhost:8081/actuator/refresh
```

## 配置文件修改流程

1. 修改 `config-server/src/main/resources/config-repo/` 下对应的配置文件
2. 重启配置中心服务
3. 刷新需要更新配置的微服务

## 本地开发

### 启动配置中心

```bash
cd config-server
mvn spring-boot:run
```

### 启动其他服务

其他服务会自动从配置中心获取配置，无需修改本地配置文件。

## Docker部署

使用docker-compose启动所有服务:

```bash
docker-compose up -d
```

配置中心会自动启动并从本地配置文件读取配置。

## 注意事项

1. 配置中心必须先于其他微服务启动
2. 各服务通过 `application.yml` 中的 `spring.config.import` 配置连接到配置中心
3. 配置修改后需要手动刷新客户端才能生效
4. 配置中心使用Basic认证保护配置端点
5. 使用 `optional:` 前缀可以在配置中心不可用时允许服务启动（使用本地配置）
