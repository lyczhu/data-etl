# ETL 数据接入系统

一个基于 Spring Cloud 微服务架构的 ETL 数据接入系统，使用 Kafka 作为消息队列解耦接入流程，Redis 作为缓存与状态协调，PostgreSQL + PostGIS 作为存储。

## 项目结构

```
data-etl/
├── registry/                     # 服务注册中心
├── data-source-service/          # 数据源管理服务
├── task-management-service/      # 任务管理服务
├── data-extraction-service/      # 数据拉取服务
├── data-transformation-service/  # 数据转换服务
├── data-storage-service/         # 数据存储服务
├── db/                           # 数据库脚本
│   ├── init/                     # 数据库初始化脚本
│   └── schema/                   # 数据库表结构脚本
├── pom.xml                       # 父项目依赖管理
└── start.sh                      # 启动脚本
```

## 系统架构

1. **数据源管理服务**：管理数据源连接信息，提供 REST API 进行增删改查。
2. **任务管理服务**：创建和管理 ETL 任务，使用定时任务扫描新任务，发送到 Kafka，并通过 Redis 跟踪任务状态。
3. **数据拉取服务**：接收任务请求，从源数据库拉取数据，发送到 Kafka。
4. **数据转换服务**：接收原始数据，进行并行转换处理，发送到 Kafka。
5. **数据存储服务**：接收转换后的数据，存储到目标数据库，发送存储完成消息到 Kafka。
6. **服务注册中心**：使用 Eureka 实现服务注册与发现。

## 技术栈

- **微服务框架**：Spring Cloud
- **服务注册与发现**：Netflix Eureka
- **消息队列**：Apache Kafka
- **缓存与状态管理**：Redis
- **数据库**：PostgreSQL + PostGIS
- **ORM 框架**：Spring Data JPA
- **构建工具**：Maven

## 环境要求

- JDK 11+
- Maven 3.6+
- Kafka 2.8+
- Redis 6+
- PostgreSQL 13+
- PostGIS 3+

## 快速开始

1. **编译项目**

```bash
mvn clean install
```

2. **启动服务**

```bash
./start.sh
```

### 启动监控堆栈

项目已集成 Prometheus/Grafana/Alertmanager/Node Exporter 的本地监控堆栈，配置文件位于 `monitoring/` 目录。启动监控组件：

```bash
docker-compose up -d prometheus alertmanager node-exporter grafana
```

验证 Prometheus 是否可用： http://localhost:9090

验证 Grafana： http://localhost:3000 （默认 admin/admin）

检查某个微服务的 Prometheus 指标暴露（以 `data-extraction-service` 为例）：

```bash
curl http://localhost:8083/actuator/prometheus
```

### 告警与仪表盘

- Prometheus 告警规则保存在 `monitoring/prometheus/rules/alerts.yml`，Prometheus 配置文件 `monitoring/prometheus/prometheus.yml` 已引用该规则文件。
- Alertmanager 配置文件：`monitoring/alertmanager/alertmanager.yml`（当前使用空接收器示例，可根据需要修改为邮件/Slack/webhook）。
- Grafana 仪表盘示例位于 `monitoring/grafana/dashboards/etl_dashboard.json`，并通过 `monitoring/grafana/provisioning` 自动加载。

1. **访问服务**

- 服务注册中心：http://localhost:8761
- 数据源管理服务：http://localhost:8081/api/data-sources
- 任务管理服务：http://localhost:8082/api/tasks

## 使用说明

1. **创建数据源**

使用数据源管理服务的 REST API 创建数据源：

```bash
curl -X POST -H "Content-Type: application/json" -d '{"name":"test-db","type":"postgresql","url":"jdbc:postgresql://localhost:5432/test","username":"test","password":"test","driverClassName":"org.postgresql.Driver"}' http://localhost:8081/api/data-sources
```

2. **创建接入任务**

使用任务管理服务的 REST API 创建接入任务（`taskId` 与目标表名由服务自动生成，目标表名规则为 `源表名_任务id前8位`）：

```bash
curl -X POST -H "Content-Type: application/json" -d '{"dataSourceId":1,"sourceTableName":"source_table"}' http://localhost:8082/api/tasks
```

3. **启动任务**

任务会自动被定时任务扫描并启动，状态流转为 `new → processing → running → completed`（异常时为 `failed`）。

4. **查看任务状态**

使用任务管理服务的 REST API 查看任务状态：

```bash
curl http://localhost:8082/api/tasks
```

## 核心流程

1. 用户使用数据源管理创建数据源
2. 用户使用任务管理创建接入任务，任务状态为 "new"
3. 任务管理模块定时扫描状态为 "new" 的任务，将任务信息发送到 Kafka，并将任务状态修改为 "running"
4. 数据拉取服务接收到任务消息，从源数据库拉取数据，发送到 Kafka
5. 数据转换服务接收到原始数据消息，进行并行数据转换，发送转换后数据消息
6. 数据存储服务接收到转换后数据消息，将数据存储到目标表，保存完成后发送数据入库消息
7. 任务管理服务接收到数据入库消息，更新 Redis 中的计数，如全部分片都已入库，更新任务状态为 "completed"

## 容错机制

- 拉取失败：`data-extraction-service` 消费者有界重试（≤3）；耗尽后向 `task-failed` 主题发失败通知，`task-management-service` 消费后将任务回滚为 `failed`
- 消息重复/丢失：Kafka 幂等生产 + 消费端去重（基于 `taskId + 主键 id` 的 Redis 幂等标记，TTL 24h）
- 入库失败：`data-storage-service` 指数退避重试（最多 3 次），仍失败则抛出交由 Kafka 重试
- 转换发送失败：`data-transformation-service` 通过 `kafkaSendRetryTemplate`（spring-retry）重试
- 日志记录：详细记录任务运行日志、异常信息

## 数据拉取策略

基于主键游标的分批拉取：按 `id` 递增，每批执行 `WHERE id >= ? LIMIT batch-size`（默认 1000），逐行封装为一条 Kafka 消息发送到 `raw-data`。

> 说明：README 早期描述的“按主键范围分片 + CompletableFuture 并行拉取”在当前实现中**未启用**，实际为单消费者顺序游标拉取；`totalShards` 参数亦未在任务实体中建模。

## 网关与鉴权

系统包含 `gateway-service`（默认端口 **8080**，已从早先与 `data-source-service` 冲突的 8081 调整而来）。网关已接入 `user-service` 的路由（`/api/auth/**`、`/api/users/**`、`/api/roles/**`），并通过 `JwtAuthGlobalFilter` 做统一 JWT 鉴权：白名单（登录、`/actuator/**`）外的请求需携带 `Authorization: Bearer <token>`，token 由 `user-service` 使用相同密钥签发。

## 注意事项

1. 首次运行前请确保 Kafka、Redis 和 PostgreSQL 服务已启动
2. 请根据实际情况修改各服务的配置文件
3. 生产环境中请调整线程池大小和并行度，以适应实际数据规模
4. 建议为各服务配置适当的日志级别和监控