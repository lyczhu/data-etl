# data-etl 架构补充风险清单与技术选型对比

> 产出方式：`risk-quality-reviewer`（补充风险）+ `graphviz`（选型矩阵图）。
> 定位：本文是《ETL 架构演进方案》（`docs/architecture/`）与 `architecture-overview.md` 的**补充件**，不重复其已覆盖内容。
> 视图状态：风险均为 **current-state**（有代码/配置证据）；方案候选为 **proposed/target**，最终选型未定。
> 配套图：`risk-tech-selection-matrix.dot`（痛点 → 方案候选 → 取舍）。渲染：`dot -Tsvg risk-tech-selection-matrix.dot -o risk-tech-selection-matrix.svg`（本机未装 Graphviz，故仅存图源）。
> 置信度：标注 [高]=直接代码/配置证据；[中]=多信号一致；[假设]=需运行期验证。

## 0. 与既有文档的关系

`architecture-overview.md §5` 与演进方案已按痛点归组为：

- A 链路长 / 串行、B 中间失败难定位、C 任务结束难判断、D 失败恢复难、E 数据接入面窄、F 微服务过度、G 治理。

**本文补充 H~M 六个新维度**（正确性、并行度、安全、高可用运维、契约演进、数据治理），并回答"除自研方案外，业界常用商业/开源产品如何覆盖 A~M"。

---

## 1. 补充风险清单（H~M）

严重度沿用 P0（会静默出错/安全事故/阻断）> P1（显著退化）> P2（隐患/维护成本）。

### H. 数据正确性与质量（"不准确"的根因，此前未覆盖）

| 编号 | 严重度 | 置信 | 问题 | 证据 |
|---|---|---|---|---|
| H1 | P0 | 高 | 转换阶段**破坏性改写**：所有数值一律四舍五入到 2 位小数，`BIGINT/DECIMAL` 先转 `double` 丢精度，金额/坐标/ID 被静默篡改 | `data-transformation-service/.../DataTransformationService.java:67-71` |
| H2 | P0 | 高 | 抽取**强依赖名为 `id` 的连续整型主键**：`WHERE id >= ? LIMIT ?` + `MIN/MAX/COUNT(id)`；无 id/UUID/复合/字符串主键表直接失效或全表重扫；去重键同样只认 `id` | `data-extraction-service/.../DataExtractionService.java:64-65,90`；`data-storage-service/.../DataStorageService.java:117-123` |
| H3 | P1 | 高 | geometry 类型退化：建表把 geometry 列**写死** `geometry(polygon,4326)`，Point/Line/其他 SRID 建错；WKB/WKT 用"首字符是否数字"启发式判别，脆弱 | `task-management-service/.../TaskScheduler.java:176-178`；`DataStorageService.java:190-201` |
| H4 | P1 | 高 | 跨库类型映射**直接拷贝源库 `TYPE_NAME`**：MySQL 类型名未必是合法 PG 类型，异构建表易失败 | `TaskScheduler.generateCreateTableSql` |
| H5 | P1 | 中 | 日期处理脆弱：仅正则 `^\d{4}-\d{2}-\d{2}$` 识别日期；无时区约定；时间戳/带时区场景错乱 | `DataStorageService.java:134-135` |
| H6 | P2 | 高 | 无数据质量校验（非空/唯一/值域/行数对账），脏数据直接入库 | 全链路无质量校验组件 |

### I. 并行度 / 吞吐天花板（比"链路长"更致命的隐藏瓶颈）

| 编号 | 严重度 | 置信 | 问题 | 证据 |
|---|---|---|---|---|
| I1 | P0 | 高 | **Kafka 以 `taskId` 作分区键 = 单任务串行**：一个任务所有行落同一 partition，`raw-data`/`transformed-data` 无论多少消费实例只能以单 partition 速度处理该任务 → 扩 worker 无效，单任务吞吐被钉死 | `DataExtractionService.sendBatchToKafka:224`；`DataTransformationService.java:48`（key 均为 taskId） |
| I2 | P1 | 高 | 生产端号称"幂等"但**未开 `enable.idempotence`**：仅 `acks=all, retries=3`，会产生重复/乱序，与 README "Kafka 幂等生产" 不符 | `config-repo/data-extraction-service.yml:35-40` |
| I3 | P1 | 高 | **无源端背压**：抽取远快于转换/入库，Kafka lag 无界堆积；`earliest` 重放放大内存与重复风险 | `auto-offset-reset: earliest` + 无流控 |

### J. 安全 / 合规（演进方案完全未涉及）

| 编号 | 严重度 | 置信 | 问题 | 证据 |
|---|---|---|---|---|
| J1 | P0 | 高 | **源库凭据明文进 Kafka 与 HTTP**：`task-request` 消息体携带 `url/username/password`，跨服务明文传输并落 topic | `TaskScheduler.java:99-108`（`EtlTaskMessage.DataSource` 含 password） |
| J2 | P0 | 高 | 数据源口令 **DB 明文存储**，REST/Feign 原样读出 | `data-source-service/.../entity/DataSource.java:17`（无脱敏/加密） |
| J3 | P1 | 高 | **JWT 密钥硬编码且入库(Git)**：同一弱 secret 写死于两处；config-server 用 `config/config123` Basic Auth | `config-repo/user-service.yml:29-30`、`config-repo/gateway-service.yml:88-89` |
| J4 | P2 | 高 | 生产日志 DEBUG（含 `org.springframework.security: DEBUG`）：泄露敏感信息、日志膨胀 | `user-service.yml:34-37`、`gateway-service.yml:80-84` |
| J5 | P2 | 中 | 动态 DDL/INSERT **全字符串拼接**，表名/列名/值未参数化白名单，存在注入面 | `TaskScheduler.generateCreateTableSql`、`DataStorageService.buildDynamicBatchInsertSqlWithGeometryHandling` |

### K. 高可用 / 运维

| 编号 | 严重度 | 置信 | 问题 | 证据 |
|---|---|---|---|---|
| K1 | P1 | 高 | **Kafka/Redis/Postgres 全单节点、RF=1**：任一挂掉即全链路停摆，无 HA | `docker-compose.yml`（KRaft 单机、`*_REPLICATION_FACTOR=1`） |
| K2 | P1 | 高 | Java 服务**不在编排内**：靠宿主机脚本启动，无健康探针/滚动/自愈 | `docker-compose.yml` registry/config 段被注释；`start.sh`、`start-service.*` |
| K3 | P2 | 中 | 消费者无优雅停机：批处理中途 kill 会重复消费或丢进度（无 checkpoint 兜底） | 无 `ContainerStoppingErrorHandler`/shutdown 钩子证据 |
| K4 | P2 | 高 | 版本漂移：`kafka-clients 3.3.2` vs broker `apache/kafka:4.1.0`；Spring Boot 2.7.18（维护尾） | 父 `pom.xml:17,21`；`docker-compose.yml:77` |

### L. 契约 / 可演进性

| 编号 | 严重度 | 置信 | 问题 | 证据 |
|---|---|---|---|---|
| L1 | P1 | 高 | Kafka 消息**无 schema 契约**：手工 JSON String 序列化，字段改名/类型变更静默破坏下游，无 Schema Registry | 各 `KafkaConfig.java`（String（De）Serializer） |
| L2 | P2 | 高 | **网关路由前缀/单复数与 Controller 不匹配**：路由 `/api/data-source/**`(StripPrefix=1)、`/api/task/**`(StripPrefix=0) vs 实际 `/api/data-sources`、`/api/tasks`，经网关会 404 | `gateway-service-routes.yml:6-36` vs `DataSourceController.java:12`、`EtlTaskController.java:12` |
| L3 | P2 | 高 | **resilience4j 全配但 0 使用**：路由未挂 `CircuitBreaker` filter，Feign 无超时/降级，下游慢即级联雪崩 | `gateway-service.yml:46-78`；`routes.yml` 无 CircuitBreaker 断言 |

### M. 数据治理 / 生命周期

| 编号 | 严重度 | 置信 | 问题 | 证据 |
|---|---|---|---|---|
| M1 | P1 | 高 | 目标表无治理：命名 `源表名_taskId前8位`，每任务建一张新表，无清理/归档/血缘，长期累积脏表 | `TaskScheduler.generateCreateTableSql`；README 建表规则 |
| M2 | P2 | 高 | 去重窗口 = Redis TTL 24h：跨 24h 重试去重失效；Redis 挂即全量重复 | `DataStorageService.java:45,86` |

### H~M 优先级建议（先做会静默出错/涉安全的）

1. **立即（不依赖重构）**：J1/J2/J3（凭据与密钥治理）、H1/H2（数据正确性）、I1（分区键策略）。这几项即便维持现架构也须先修，且改动局部。
2. **随演进一起做**：I2/I3、L1、H3~H5、D/C（已有 checkpoint/状态机方案承接）。
3. **容量/合规驱动时**：K1/K2、M1、J4/J5、L2/L3。

---

## 2. 业界方案如何覆盖 A~M

按能力分类给出**开源 + 商业/云**候选，并对"本项目单团队、≤30M 行/表、PostgreSQL+PostGIS 目标、避免过度工程"的规模给出取舍。

### 2.1 数据集成 / CDC / ELT 平台（治 E 接入面 + A 链路 + D 恢复 + H 正确性）

- **开源**：**Apache SeaTunnel**（source-transform-sink，原生 JDBC/Mongo/Kafka/文件 connector，checkpoint 精确一次，可跑自带引擎或 Flink/Spark）、**Apache NiFi**（可视化流，**内置背压 / provenance 血缘 / 失败自动重试与路由 / 丰富连接器**）、**DataX**（阿里，离线批式异构同步）、**Debezium**（CDC 增量）、Kettle/Pentaho。
- **商业/云**：**Fivetran**、**Airbyte**（OSS+云）、Talend / Informatica IDMC、Qlik Replicate、Oracle GoldenGate、AWS DMS、Azure Data Factory、Matillion。
- **取舍**：文件/GeoJSON/NoSQL 接入且少维护 → **SeaTunnel / NiFi 最佳**。NiFi 直接贴合"背压 + 失败处置 + provenance 可观测"三痛点；SeaTunnel 强在"多源 connector + 断点续传"。**不建议** Fivetran/Informatica（成本高、PostGIS/GeoJSON 空间语义弱）。

### 2.2 批处理引擎（治 A 吞吐 + I 并行 + D 恢复）

- **开源**：**Spring Batch**（chunk + 分页 + checkpoint + retry/skip + restart + `Partitioner` 分区并行，**与现有 Spring 栈零摩擦**）、Apache Spark、MapReduce、Hive。
- **商业/云**：Databricks、AWS Glue/EMR、Cloudera。
- **取舍**：若倾向"用成熟框架替代自研可靠性轮子" → **Spring Batch 是合并 E-T-L 的最佳落地**：step 级 restart/skip/retry 原生解决 D、`Partitioner` 解决 I1/I3（并行）。2~3 千万/表用 Spring Batch 刚好；Spark 对本规模偏重。

### 2.3 流处理 / 流批一体（治 E2 IoT/流式 + I 并行）

- **开源**：**Apache Flink**（DataStream/Table 流批一体，exactly-once checkpoint/savepoint，丰富 State）、**Kafka Streams**（库形式嵌入现有 JVM，无需独立集群，原生 DLT/serde 生态）、Spark Structured Streaming、ksqlDB、Pulsar+Flink、RisingWave/Materialize。
- **商业/云**：**Confluent Platform**（Kafka+Flink+Tableflow+Schema Registry 一体）、**Ververica Platform(VVP)**、AWS Managed Streaming for Apache Flink、阿里云实时计算 Flink、GCP Dataflow。
- **取舍**：IoT/低延迟首选 **Kafka Streams**（复用现有 Kafka + Java 栈，最轻）。仅当需要复杂窗口/大状态/高吞吐才上 **Flink**；为单独立 Flink 集群对本项目运维成本超过收益。

### 2.4 编排 / 状态机 / 工作流（治 C 难判断 + D 恢复 + 状态模型落地）

- **开源**：**Temporal / Cadence**（**持久化工作流：代码即状态机、自动重试退避、可取消/挂起、可续跑(确定性重放)、执行历史可观测**）、DolphinScheduler（可视化 DAG、重试、告警）、Airflow、Dagster、Prefect、Azkaban、Oozie。
- **商业/云**：**Temporal Cloud**、Astronomer / AWS MWAA、Prefect Cloud、Netflix Conductor / Orkes、AWS Step Functions。
- **取舍**：**Temporal 与演进方案 §3/§4 的"自研状态机 + checkpoint + outbox"高度同构**——可直接承接 `CREATED→…→NEEDS_ATTENTION`、重试退避、`/cancel`、断点续跑、执行历史。评估"用 Temporal 承载编排/恢复，worker 只做纯 E-T-L"可省掉大量自研可靠性代码。DolphinScheduler/Airflow 偏"定时批调度"，对"单任务内失败处置/续跑"帮助有限。

### 2.5 湖仓 / 存储格式（治幂等 upsert + 可回溯）

- **开源**：Apache Iceberg / Hudi / Delta Lake / Paimon（upsert、time travel、schema evolution、增量 CDC sink）；OLAP：ClickHouse / Apache Doris / StarRocks。
- **商业/云**：Snowflake(Snowpipe)、Databricks、BigQuery。
- **取舍**：目标库是 **PostgreSQL + PostGIS（关系+空间）**，湖仓格式契合度低 → **不建议引入**；用 PG 原生 `MERGE`/`ON CONFLICT` + 分区表即达幂等 upsert。仅未来做海量分析再考虑 Doris/CK。

### 2.6 IoT / 时序接入（治 IoT 场景）

- **开源**：EMQX / HiveMQ（MQTT broker + rule-engine 桥接 Kafka）、Kafka Connect MQTT Source、Apache IoTDB、TimescaleDB、InfluxDB。
- **商业/云**：AWS IoT Core、Azure IoT Hub、Confluent + MQTT connector。
- **取舍**：**EMQX / Kafka-Connect-MQTT → 现有 Kafka topic → stream worker** 是最省事路径。

### 2.7 空间数据（GeoJSON/PostGIS 专项，治 H3）

- **开源**：**GeoTools**（解析 GeoJSON/SHP → WKT/WKB，交现有 PostGIS）、Apache Sedona（Spark/Flink 空间流批）、GeoMesa、GeoParquet。
- **取舍**：小批量用 **GeoTools** 足够；仅空间大数据分布式处理才上 Sedona。

### 2.8 数据质量 / 契约 / 血缘（治 H6 + L1 + B 可观测）

- 数据质量：**Great Expectations**、**deequ**、dbt tests、Evidently。
- Schema 契约：**Confluent Schema Registry / Apicurio / Karapace**（Avro/Protobuf/JSON Schema）。
- 血缘：OpenLineage / Marquez。
- **取舍**：**引入 Schema Registry(Avro) 替代手工 JSON 是性价比最高项**（直接消除 L1，缓解 B）。质量校验可选 Great Expectations 轻量接入做入库前后对账（治 C 的"是否结束/是否准确"）。

### 2.9 可观测 / 追踪（治 B + 落地演进方案 §6）

- **开源**：**OpenTelemetry**（统一 trace/metric/log）、Grafana LGTM（Loki+Grafana+Tempo+Mimir）、Jaeger/Zipkin；已有 Prometheus/Alertmanager。
- **商业**：Datadog、Grafana Cloud、Elastic APM。
- **取舍**：复用现 Prometheus/Grafana 栈，**补 OpenTelemetry + Tempo（trace）** 即闭环，不换 APM。

---

## 3. 选型组合建议（贴合"务实、不过度工程"）

| 组合 | 栈 | 覆盖 | 代价 |
|---|---|---|---|
| **A. 框架化（推荐评估）** | Spring Batch（E-T-L 合并 + chunk/checkpoint/restart/skip + Partitioner）+ Temporal（编排/状态机/重试/取消续跑/历史）+ Kafka Streams（IoT 流式）+ Schema Registry(Avro) + OpenTelemetry/Tempo + Vault/Jasypt | A/C/D/B/H/I(并行)/E(IoT)/J/L | 学 Temporal 成本；无独立 Flink 集群 |
| **B. 平台化** | Apache NiFi 或 SeaTunnel 承担多源+文件+背压+失败路由+provenance；orchestrator 仅做登记/汇总；Kafka 只做 IoT/流式通道 | E/H(连接器免自研)/I(背压)/B(provenance) | 引入新平台；PostGIS/GeoJSON 语义需验证 |
| **C. 自研（演进方案原案）** | etl-worker(进程内 E-T-L) + 自研状态机 + checkpoint + outbox | 全含 | 可靠性代码/测试自扛，维护面最大但可控性最强 |

推荐落地倾向：

1. **安全项（J1/J2/J3）无条件先做**，用 **Vault/KMS + Jasypt**（配置加密）+ 凭据不落 Kafka（改传 `dataSourceId`，worker 侧现取现用、内存态、审计），与是否重构无关。
2. **以演进方案的自研架构为骨架**（贴合现有 Spring Cloud 代码、改动可控），但**用成熟件替换最难的可靠性轮子**：
   - 状态机/重试/续跑/取消/历史 → 评估 **Temporal**；或退而用 **Spring Batch** 的 step restart/skip/retry。
   - 多源接入(文件/NoSQL/GeoJSON)/并行分片 → 评估 **Spring Batch Partitioner + GeoTools**，或 **SeaTunnel/NiFi** 连接器，避免自研 H1~H5 解析坑。
   - 消息契约 → **Schema Registry(Avro)**（近零成本解决 L1、改善 B）。
   - IoT 流式 → **Kafka Streams**（复用现有 Kafka 与 JVM）。
3. **明确不建议**（当前规模过度工程）：独立 Flink 集群、Iceberg/Hudi 湖仓、重型商业 SaaS（Fivetran/Informatica/GoldenGate）。仅作为规模/团队扩张后的备选。

---

## 4. 假设与待验证

- [假设] 并发任务数 ≤ 20、单表 ≤ 30M 行、单团队 ≤ 5 人（与演进方案一致）；否则 Temporal/Flink 的取舍需重估。
- [待验证] I1 分区键对吞吐的实际抑制幅度：需先补 §2.9 指标（`kafka_consumer_..._records_lag`、per-topic 吞吐）再定量确认。
- [待验证] L2 网关路由 404：需 `curl` 经网关逐路由验证；若确证为 P1（影响对外 API）。
- [待验证] H4 异构建表：需一个 MySQL→PG 的实际源表样本跑通确认类型映射失败面。

## 5. 证据索引（本文新增）

`DataTransformationService.java:67-71` · `DataExtractionService.java:64-65,90,224` · `DataStorageService.java:45,86,117-123,134-135,190-201` · `TaskScheduler.java:99-108,176-210` · `DataSource.java:17` · `config-repo/{user-service,gateway-service,data-extraction-service,gateway-service-routes}.yml` · `docker-compose.yml` · 父 `pom.xml`
