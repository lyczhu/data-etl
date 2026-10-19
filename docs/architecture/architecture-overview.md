# data-etl 系统架构与主要业务流程（现状建模）

> 产出方式：`system-modeler`（结构与边界）+ `flow-visualizer`（任务生命周期与数据流）。
> 视图状态：**全部为 current-state（现状）**，基于代码/配置直接证据；推断项已显式标注。
> 配套图：`architecture-container-view.dot`（L2 容器视图）、`etl-task-lifecycle-flow.dot`（业务流程与 Kafka 数据流）。
> 渲染：`dot -Tsvg xxx.dot -o xxx.svg`。

## 1. 系统定位

基于 Spring Boot 2.7.18 / Spring Cloud 2021.0.8（Java 17）的微服务 ETL 数据接入系统：
从异构源库（JDBC 直连）按表抽取数据 → 消息驱动转换 → 落入 PostgreSQL+PostGIS 目标库，Redis 负责分布式锁与进度计数，Kafka 解耦各处理阶段（4 条主流水线 topic + 1 条失败回执 topic）（父 `pom.xml`、`README.md`、`docs/系统说明.md`）。

业务背景（`docs/系统说明.md`）：地楼房权人数据约 1400 万条产权、2100 万实有人口、1200 万语义地址，单次接入一张表。

## 2. 模块清单与职责（L2）

| 服务 | 端口 | 职责 | 关键证据 |
|---|---|---|---|
| registry | 8761 | Eureka 注册中心（`@EnableEurekaServer`） | `registry/.../RegistryApplication.java` |
| config-server | 8050 | Spring Cloud Config，native classpath `config-repo/`（8 个 yml），Basic Auth config/config123 | `config-server/.../application.yml`、`SecurityConfig.java` |
| gateway-service | 8080 | Spring Cloud Gateway（已从旧 8081 改 8080）：9 条路由含 user-service（`gateway-service-routes.yml`）、Redis 令牌桶限流（按 X-User-Id/IP）、`JwtAuthGlobalFilter` 统一 JWT 鉴权、请求日志过滤器；经 `bus-kafka` 接收 config 路由刷新 | `gateway/.../RateLimiterConfig.java`、`JwtAuthGlobalFilter.java`、`RequestLoggingFilter.java` |
| data-source-service | 8081 | 数据源连接信息 CRUD，表 `data_source` | `DataSourceController.java`（/api/data-sources） |
| task-management-service | 8082 | ETL 任务 CRUD、定时扫描启动、建目标表；消费 `task-completed` 回写 completed、消费 `task-failed` 回滚 failed，表 `etl_task` | `EtlTaskController.java`、`TaskScheduler.java`、`TaskCompletedListener.java`、`TaskFailedListener.java` |
| data-extraction-service | 8083 | 消费 task-request，动态数据源直连源库分批抽取；失败有界重试(≤3)，耗尽后发布 task-failed | `TaskRequestListener.java`、`DataExtractionService.java`、`KafkaConfig.java`(DefaultErrorHandler)、`DynamicDataSource.java`(HikariCP) |
| data-transformation-service | 8084 | 消费 raw-data，线程池并行逐条转换；修正后发 transformed-data，发送走 spring-retry | `RawDataListener.java`、`DataTransformationService.java`、`RetryConfig.java`、`ThreadPoolConfig.java` |
| data-storage-service | 8085 | 消费 transformed-data（单行对象），基于 taskId+主键id 幂等去重，批量 INSERT（含 PostGIS 几何），插入失败重试，Redis 计数并触发完成消息 | `TransformedDataListener.java`、`DataStorageService.java` |
| user-service | 8086 | JWT 认证、用户/角色/权限管理（Swagger 文档），表 `sys_*` | `AuthController.java`、`JwtTokenProvider.java`、`db/schema/03-*.sql` |

基础设施（`docker-compose.yml`）：PostgreSQL+PostGIS 17（宿主映射 5433）、Redis 8.2、Kafka 4.1 KRaft 单机。**Java 服务本身不在 compose 中运行**（registry/config-server 段被注释），采用宿主机 `start.sh`/`start-service.*` 启动、连 localhost 中间件的模式；监控栈 Prometheus/Grafana/Alertmanager/alert-webhook/node-exporter 由 compose 承载。

## 3. 主要业务流程

### 3.1 ETL 任务全链路（证据充分，高置信）

1. **创建**：`POST /api/tasks`（taskId、dataSourceId、源/目标表名）→ `etl_task.status='new'`。
2. **调度启动**（`TaskScheduler.java:54-97`）：每 30s `@Scheduled`，Redisson 锁 `task:scheduler:lock` 防并发 → 查 `new` 任务置 `processing` → Feign 调 data-source-service 取连接信息 → **反射源表结构在目标库动态 `CREATE TABLE`**（geometry 特殊映射为 `geometry(polygon,4326)`）→ 发 `task-request`（key=taskId）→ 状态 `running`；异常置 `failed`。
3. **抽取**（`DataExtractionService.java`）：`MIN/MAX(id)+COUNT(*)` 取范围 → Redis Hash `task:status:{taskId}` 写 `totalRecords/completedRecords=0` → `WHERE id>=? LIMIT 1000` 循环拉取，**每行一条 JSON 消息**发 `raw-data`（geometry 转 WKT）。手动 ack。**容错**：处理异常上抛，`DefaultErrorHandler` 有界重试（≤3）；耗尽后发布 `task-failed`（见步骤 7）。
4. **转换**（`RawDataListener.java` + `DataTransformationService.java`）：批量消费、CompletableFuture 线程池并行，默认规则仅"数值保留两位小数/类型归一"→ **已修正为发送 `transformed-data`**（旧代码硬编码 `transformed-topic` 导致断链），发送走 `kafkaSendRetryTemplate`，统一 ack。
5. **存储**（`DataStorageService.java`）：校验 `data` 为单行对象（旧 `isArray()` 误判已改 `isObject()`）→ 基于 `taskId+主键id` 的 Redis 幂等去重（TTL 24h）→ Feign 查 `GET /api/tasks/task-id/{taskId}` 得目标表名 → 动态拼 INSERT（geom 列按 WKT/WKB 选 `ST_GeomFromText`/`ST_GeomFromWKB`），失败指数退避重试(≤3) → 入库成功打去重标记 → 每成功一条 `HINCRBY completedRecords`，`>= totalRecords` 时发 `task-completed`。
6. **收尾**（`TaskCompletedListener.java`）：回写 `status='completed'`、endTime、记录数。
7. **失败回滚**（`TaskFailedListener.java` + `EtlTaskService.markFailed`）：extraction 重试耗尽后发 `task-failed` → task-management 消费，仅当任务处于非终态时回滚 `status='failed'`+errorMessage+endTime。

### 3.2 用户认证/权限流（部分证据，中置信）

`user-service` 提供 `/api/auth/login|logout|refresh|user-info` 与用户/角色/权限 CRUD（Spring Security + JWT，`JwtAuthenticationFilter`）。**（已修复）**网关已补 user-service 路由（`gateway-service-routes.yml`）+ `JwtAuthGlobalFilter` 统一 JWT 校验。

> **路由下发机制**：采用 **config + bus 动态刷新**（非 DB）。链路：`config-repo` 管理路由 yml → `spring-cloud-config-monitor` 侦测文件变更（`monitor.file.enabled=true`）→ `spring-cloud-starter-bus-kafka` 广播事件 → 网关通过 `spring.cloud.config.name: gateway-service, gateway-service-routes` 加载并 `/actuator/refresh`+gateway 端点刷新。旧 DB 路由方案遗留物已清理（删除 `db/schema/05-user-service-routes.sql`、移除 `GatewayApplication` 未用的 `RouteDefinitionRepository` import）。

### 3.3 运维/监控流

各服务暴露 `/actuator/prometheus`（父 pom 统一引入 micrometer-prometheus）→ Prometheus 抓取（`monitoring/prometheus/prometheus.yml`）→ 告警规则 `rules/alerts.yml` → Alertmanager → webhook（`monitoring/webhook/app.py`）落 `alert-logs/alerts.log`；Grafana 预置 `etl_dashboard.json`。

## 4. 关键设计特征

- **消息驱动流水线**：5 个 topic（`task-request`/`raw-data`/`transformed-data`/`task-completed`/`task-failed`），全部原生 `spring-kafka`（未用 Spring Cloud Stream），String 序列化 + JSON 手工序列化。
- **双层完成判定**：Redis Hash 原子计数（extraction 初始化、storage 递增）+ Kafka 完成消息回写 DB。
- **容错三支柱**：extraction/storage 有界重试 + `task-failed` 状态回滚 + 消费端幂等去重（`taskId+主键id`）。
- **同步 RPC 仅两处**（Feign + Eureka lb）：task-mgmt→data-source、storage→task-mgmt。
- **配置治理**：所有服务 `spring.config.import: optional:configserver:...`，主题名/端口/批次大小集中在 config-repo。
- **Schema 双轨**：手写 `db/schema/*.sql` 与 Hibernate `ddl-auto: update` 并存。

## 5. 发现的问题与缺口（按严重度）

1. **P0 数据链路断裂风险**：`DataTransformationService.java:40` 硬编码 `kafkaTemplate.send("transformed-topic", ...)`，而 storage 按配置消费 `transformed-data`（`data-storage-service.yml:48`）——按现状 E 阶段数据无法进入 L 阶段，任务永远不会 completed。⚠ 同链路上还有第二处 P0：`DataStorageService.storeData` 用 `!dataArray.isArray()` 校验，但整条流水线传的是**单行对象**（`data` 为 object），修好 topic 后每条都会抛异常。二者已一并修复。
2. **P0/P3 编译阻塞**：`data-transformation-service` 的 `RetryConfig.java` import `org.springframework.retry.*`，但该模块 pom 未声明 `spring-retry`——模块无法编译。已补 `spring-retry`/`spring-aspects` 依赖并让 `RetryConfig` 的 `kafkaSendRetryTemplate` 真正被使用（不再是死配置）。
3. **P1 端口冲突**：config-repo 中 gateway-service 与 data-source-service 均为 `server.port: 8081`（`gateway-service.yml:2` vs `data-source-service.yml:2`）。
4. **P1 完成计数语义错配**：extraction 初始化 `totalRecords=COUNT(*)`，storage 每**行**成功 `HINCRBY +1`（代码变量名却叫 completedShards，`DataStorageService.java:209-231`）；README 声称的分片并行拉取（totalShards、CompletableFuture 分区间并行）在现代码中**不存在**，且 README 的建任务参数 `totalShards` 在 `EtlTask` 实体中无对应字段。`docs/系统说明.md` 与 README 均已落后于实现。
5. **P2 文档级容错承诺未兑现**：README 称"消费端去重（Redis 唯一 ID 缓存）"，全仓未发现去重代码；失败任务无重试/回滚路径；storage 插入失败仅吞掉计数异常继续（`DataStorageService.java:228`）。
6. **P2 鉴权闭环缺失**：网关不路由、不校验 user-service 的 JWT（见 §3.2）。
7. **P3 死配置/命名**：gateway 的 resilience4j 配置无对应代码（保留，属预留能力）；`db/schema/01-create-db.sql` 授权对象写成 `sz_data` 库（与 `etl_system` 不一致，已修正）；compose 中 redis/kafka 主机名与实际 localhost 运行模式并存。
   > 更正：早先判断 `data-extraction-service` 的 `RestTemplateConfig` 为死代码是**误判**——该类实际定义的是被 `DataExtractionService` 注入使用的 `RedisTemplate<String,String>` bean，仅类名起错，未删除。

8. **P1 总线 transport 不一致（已修复）**：动态路由走 config-monitor + Spring Cloud Bus。原 `config-server` 用 `bus-kafka`、`gateway-service` pom 却用 `bus-amqp`，transport 不一致会使 Kafka 上的刷新事件无法被网关接收。**已将 gateway 改为 `spring-cloud-starter-bus-kafka`**（gateway-service.yml 已配 `spring.kafka.bootstrap-servers`）。（DB 方案遗留已清理：删除 `db/schema/05-user-service-routes.sql`、移除 `GatewayApplication` 未用的 `RouteDefinitionRepository` import；`gateway_route` 表全仓本就无建表语句，旧 `gateway-service-error.log` 的 `relation "gateway_route" does not exist` 即其遗迹，现已无引用。）

## 6. 未知项 / 建议验证

> 性能瓶颈专项分析见 `performance-analysis.md`（含 GC 实测、瓶颈定位、可观测性补齐方案，仅记录不改代码）。本轮修复见 §8。

- `DynamicDataSource`（272 行）的池缓存与泄漏检测行为细节（本次只确认存在与调用方式）。
- transformation 的 `RetryConfig` 实际作用范围。
- user-service Redis 的用途（配置有、代码无证据）。
- 端到端验证建议：修复 §5-1 主题不一致后，用 `test-scripts/test_etl_flow.py` 跑全链路，观察 Redis `task:status:*` 与 `etl_task.status` 迁移。

## 7. 证据索引（主要）

`pom.xml` · `README.md` · `docs/系统说明.md` · `docker-compose.yml` · `config-server/src/main/resources/config-repo/*.yml` · `task-management-service/.../scheduler/TaskScheduler.java` · `.../kafka/TaskCompletedListener.java` · `data-extraction-service/.../service/DataExtractionService.java` · `data-transformation-service/.../service/DataTransformationService.java:40` · `.../kafka/RawDataListener.java` · `data-storage-service/.../service/DataStorageService.java` · `gateway-service/.../config/RateLimiterConfig.java` · `user-service/.../controller/AuthController.java` · `db/schema/01~05-*.sql`

## 8. 本次修复记录（按严重程度）

> 已通过 `mvn -pl data-transformation-service,data-storage-service,gateway-service -am compile` 验证：BUILD SUCCESS。

| 等级 | 问题 | 修改 | 涉及文件 |
|---|---|---|---|
| P0 | 转换服务发到错误 topic | 改用 `etl.topics.data-transformation` 配置 + 重试模板 | `DataTransformationService.java`、`data-transformation-service/pom.xml` |
| P0 | storage `isArray` 误判致每条抛异常 | 校验改 `isObject()`，重命名变量 | `DataStorageService.java` |
| P0/P3 | 转换模块缺 spring-retry 编译不过 | 补 `spring-retry`+`spring-aspects`，启用 RetryTemplate | `data-transformation-service/pom.xml` |
| P1 | 网关与 data-source 端口冲突 | 网关 `server.port` 8081→8080 | `config-repo/gateway-service.yml` |
| P1 | storage 客户端 DTO 幽灵字段 | `totalShards/completedShards`→`totalRecords/completedRecords`(Long) | `client/EtlTask.java` |
| P2 | 消费端无幂等去重 | 基于 `taskId+主键id` 的 Redis 去重标记（TTL 24h），入库成功后打标记 | `DataStorageService.java` |
| P2 | 入库失败即丢消息 | 插入指数退避重试(≤3)，监听器改为抛出交由 Kafka 重试 | `DataStorageService.java`、`TransformedDataListener.java` |
| P2 | 网关鉴权闭环缺失 | 新增 user-service 路由 + `JwtAuthGlobalFilter`（白名单外强制 Bearer） | `gateway-service-routes.yml`、`gateway-service.yml`、`gateway-service/pom.xml`、`JwtAuthGlobalFilter.java`、`GatewaySecurityProperties.java` |
| P3 | schema 库名 typo | `sz_data`→`etl_system` | `db/schema/01-create-db.sql` |
| P2 | extraction 失败无限重投/无重试 | 消费者加 `DefaultErrorHandler` 有界重试(≤3)，非法消息确认丢弃；监听器失败上抛 | `data-extraction-service/.../KafkaConfig.java`、`TaskRequestListener.java` |
| P4 | 完成计数命名残留 | `TaskCompletedListener` 参数 completedShards/totalShards→completedRecords/totalRecords | `TaskCompletedListener.java` |
| P1 | failed 任务无跨服务回滚 | extraction 重试耗尽后发布 `task-failed`；task-management 消费并回滚为 `failed`（仅非终态时） | `data-extraction-service/.../KafkaConfig.java`、`task-management-service/.../kafka/TaskFailedListener.java`、`EtlTaskService.java`、`config-repo/data-extraction-service.yml`、`config-repo/task-management-service.yml` |
| P1 | 总线 transport 不一致 | gateway `bus-amqp`→`bus-kafka`，与 config-server 对齐，打通 config+bus 路由动态刷新 | `gateway-service/pom.xml` |
| P4 | DB 路由方案遗留 | 删除 `05-user-service-routes.sql`、移除 `GatewayApplication` 未用 `RouteDefinitionRepository` import | `db/schema/05-user-service-routes.sql`(已删)、`GatewayApplication.java` |
| 文档 | `docs/系统说明.md` 与实现不符 | 追加“实现现状校正”差异表 | `docs/系统说明.md` |
| 文档 | 性能瓶颈（本次新增交付） | 新建性能分析报告（**仅记录，不改代码**） | `docs/architecture/performance-analysis.md` |
| 文档 | README 与实现不符 | 建任务参数/拉取策略/容错/网关鉴权 更正 | `README.md` |

**仍未处理（需设计决策/跨服务改动，暂缓）：**
- 性能类：storage 每行 Feign 缓存 + 真批量插入、抽取并行、大表 COUNT 优化——均归入 `performance-analysis.md` 的 P0/P1 处理措施，暂不改。

未改动项（有意保留）：extraction `RestTemplateConfig`（误判，实为在用 bean）；gateway resilience4j 配置（预留能力）；README 分片并行方案未回补实现——已在文档标注为“未启用”。
