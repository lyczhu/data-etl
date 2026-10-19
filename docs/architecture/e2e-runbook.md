# data-etl 端到端测试 Runbook 与报告（Phase 0）

> 产出方式：`executing-plans` + `verification-before-completion`（证据先行）。
> 目标：跑通"任务接入 → 抽取 → 转换 → 存储 → 状态回报"端到端流程，并验证《ETL 架构演进方案》Phase 0 落地项。
> 运行环境：Windows 22H2 / PowerShell 7 / Docker 29.6.1 / JDK 25.0.2（全栈无 JDK 17）/ Maven 3.9.11。
> 结论一句话：**全链路端到端跑通（8/8 行、含 PostGIS 几何与日期），Phase 0 的 storage 真批 + etl.* 指标 + PG 慢查询配置均实测生效；过程中暴露并修复了 4 个阻断链路的既有缺陷；trace 日志前缀未生效（列为待改进）。**
> 原始证据：`e2e-logs/`（各服务 out/err 日志、`status.txt`/`final-result.txt`/`redrive2-result.txt`、`seed.sql`）。

## 1. 结果摘要（证据）

| 验证点 | 结果 | 证据 |
|---|---|---|
| 全栈在 JDK 25 启动 | ✅ 8 个服务均起 | registry/config/data-source/task-mgmt/extraction/transformation/storage + infra |
| 端到端任务完成 | ✅ `status=completed completed=8 total=8` | `redrive2-result.txt` L7（task `42d3921d`） |
| 目标表数据正确 | ✅ `rows=8 polygons=8`，`row1=Alice/30/2024-01-05` | `redrive2-result.txt` L8（PostGIS 几何 + 日期无损） |
| Phase 0 storage 真批 | ✅ `etl_load_duration_seconds_count=1`（8 行单批 load） | L11；`etl_records_in/out_total=8` L10/L12 |
| Phase 0 EtlMetrics 暴露 | ✅ `/actuator/prometheus` 有 `etl_records_{in,out}`、`etl_load_duration_seconds` | curl 抓取 |
| Phase 0 Caffeine 表名缓存 | ✅ 无逐行 Feign 错误、表名解析正常 | storage 正常入库 8 行 |
| Phase 0 PG 慢查询/pg_stat_statements | ✅ `log_min_duration_statement=100ms`、`shared_preload_libraries=pg_stat_statements`、扩展已建 | `docker exec postgres psql SHOW ...` |
| Phase 0 Sleuth traceId 入日志 | ❌ 未生效 | storage 日志为默认 pattern，无 `[trace:…,task:…]`（见 §5 待改进） |

## 2. 前置条件

```powershell
# 依赖：docker（compose）、JDK 17+（本次 25）、Maven、可选 python（test-scripts）
docker --version; java -version; mvn -v
```
端口约定：infra `5433(PG)/6379(Redis)/9092(Kafka)`；服务 `8761(registry) 8050(config) 8081(data-source) 8082(task-mgmt) 8083(extract) 8084(transform) 8085(storage)`，网关 8080。

## 3. 操作步骤（Runbook）

### 3.1 启动基础设施
```bash
docker compose up -d postgres redis kafka
docker ps --format "{{.Names}}: {{.Status}}"
# 验证 Phase 0 PG 观测配置
docker exec postgres psql -U postgres -d etl_system -t -c "SHOW log_min_duration_statement;"   # 期望 100ms
docker exec postgres psql -U postgres -d etl_system -t -c "SHOW shared_preload_libraries;"      # 期望含 pg_stat_statements
docker exec postgres psql -U postgres -d etl_system -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements;"
```

### 3.2 打包（务必先释放 jar 锁）
```bash
# 关键：重打包前必须停掉占用 target/*.jar 的旧 JVM（否则 spring-boot:repackage 静默失败，产出非可执行 jar）
mvn -q -DskipTests clean package
```
> 踩坑记录（见 §4-C）：用相对路径 `java -jar x\target\y.jar` 启动时，进程命令行不含目录名，按 `*data-etl*` 匹配会漏杀，导致 `mvn clean` 因 jar 被占用而失败。正确做法：**按监听端口反查 PID 再杀**（`Get-NetTCPConnection -LocalPort <p> -State Listen`）。

### 3.3 启动服务（严格按序 + 健康门控）
依赖链决定顺序（pipeline 服务的 kafka/redis/datasource/topic 配置**全部来自 config-server**，本地 application.yml 只有端口/actuator，缺配置则启动即失败）：
```
registry(8761) → config-server(8050) → [data-source, task-management, data-extraction, data-transformation, data-storage]
```
每个服务 `java -Xms256m -Xmx512m -jar <module>/target/<module>-1.0.0.jar`，等其端口就绪再下一个。
> 注意：用 `java -jar -Xmx512m` 手动启动**不会读取 `service.conf`**，故 Phase 0 的堆 512M→1G 变更在本 E2E 未被运行时验证（仅作为 `start-service.*` 的配置项存在）。生产应经 `start-service.bat/.sh` 启动。

### 3.4 建样例源表（PostGIS）
`demo_people`（integer 主键 `id` + POLYGON `geom` + date，8 行）——脚本见 `e2e-logs/seed.sql`，经 `docker exec -i postgres psql` 灌入。**刻意用 POLYGON** 以匹配 `TaskScheduler` 把 geometry 硬编码为 `geometry(polygon,4326)` 的已知限制（问题清单 H3）。

### 3.5 驱动 ETL（REST）
```bash
# 建数据源（指向 etl_system）
curl -X POST -H "Content-Type: application/json" \
  -d '{"name":"e2e-pg","type":"postgresql","url":"jdbc:postgresql://localhost:5433/etl_system","username":"postgres","password":"postgres","driverClassName":"org.postgresql.Driver"}' \
  http://localhost:8081/api/data-sources        # 返回含 id
# 建任务（源表 demo_people）
curl -X POST -H "Content-Type: application/json" \
  -d '{"dataSourceId":<ID>,"sourceTableName":"demo_people"}' \
  http://localhost:8082/api/tasks                # 返回 taskId / targetTableName
```
task-mgmt `@Scheduled(30s)` 扫描 `new` → 建目标表 → 发 `task-request` → extraction 拉 `demo_people` 逐行发 `raw-data` → transformation 转后发 `transformed-data` → storage 真批入库 + 计数 → `task-completed` 回写 `completed`。

### 3.6 校验
```bash
curl http://localhost:8082/api/tasks/task-id/<TASKID>            # status=completed, completedRecords=8
docker exec postgres psql -U postgres -d etl_system -c "SELECT count(*), count(*) FILTER (WHERE ST_GeometryType(geom)='ST_Polygon') FROM <target>;"
curl http://localhost:8085/actuator/prometheus | grep '^etl_'     # etl_records_in/out_total, etl_load_duration_seconds
```

## 4. 端到端暴露并已修复的既有缺陷（P0，链路"跑不通"的直接原因）

> 这 4 项都不是 Phase 0 引入，而是**原架构里导致"任务永不 completed / 服务起不来"的隐患**，本次 E2E 才第一次把它们逼出来，与用户痛点高度吻合。

| # | 症状 | 根因 | 修复 | 文件 |
|---|---|---|---|---|
| A | transformation 启动即 `NoSuchBean: etlTransformExecutor` | `ThreadPoolConfig.etlTransformerExecutor()` **缺 `@Bean`**，且方法名与监听器 `@Qualifier("etlTransformExecutor")` **拼写不一致** | 加 `@Bean("etlTransformExecutor")` 并统一命名 | `data-transformation-service/.../config/ThreadPoolConfig.java` |
| B | transformation 启动即 `NoSuchBean: kafkaManualAckListenerContainerFactory` | `RawDataListener` 引用 `containerFactory="kafkaManualAckListenerContainerFactory"`，但该 batch+手动 ack 工厂**从未定义** | 新增该 `@Bean`（`setBatchListener(true)`+`AckMode.MANUAL_IMMEDIATE`） | `data-transformation-service/.../config/KafkaConfig.java` |
| C | transformation 运行期每条发送 `ConfigException: Invalid value 262,144 for batch.size` | config-repo `data-transformation-service.yml` 的 `batch.size: 262,144` **含千分位逗号**，被当字符串传给 INT 型 Kafka 配置 → E→L 断链、任务永不到 completed | 改为 `262144` | `config-server/.../config-repo/data-transformation-service.yml` |
| D | task-mgmt 启动即 `RedisConnectionException: ERR AUTH called without any password` | config-repo task-mgmt `spring.redis.password:` **空串**被 Redisson 当作 AUTH 口令，而本地/编排 Redis 无密码 | 移除空 `password` 属性 | `config-server/.../config-repo/task-management-service.yml` |

其余实测发现（未改，登记）：
- **`RawDataListener.listen` 的 `.exceptionally` 吞掉单条发送异常后仍 `ack`** → 一旦某条 send 失败（如缺陷 C 期间），数据被静默丢弃且位点提交。本次 C 修复后 happy-path 不再触发，但**根治需 Phase 1 DLT**（方案 §4）。
- **transformation/storage 消费者 `auto.offset.reset` 缺省为 `latest`**（config-repo 未设）→ 服务重启期间的 raw-data/transformed-data 会被跳过（首跑时 884e8249 的前 8 条就被跳过）。生产应显式设 `earliest` + 依赖去重（Phase 1）。
- 印证问题清单 **H3**（geometry 硬编码 polygon 4326）、**H2**（强依赖整型 `id` 主键）、**C1/C2**（Redis 计数 + `totalRecords=COUNT(*)`）。

## 5. 待改进 / 未覆盖项（诚实标注）

1. **Sleuth traceId 未入运行日志**：把 `logging.pattern.console/file` 仅放 config-repo，对 Boot 2.7 + config-server 不生效（Logback 在远端配置注入前已初始化）。**修正建议**：将 trace 日志格式下沉到各服务本地 `application.yml` 或 `logback-spring.xml`（config-server 已有），或引入 `logback-spring.xml` 到 pipeline 服务。Phase 0 该项标记为 partial。
2. **堆 512M→1G 未经运行时验证**：E2E 用 `java -jar -Xmx512m` 手动起，未读 `service.conf`。需在经 `start-service.*` 正式启动后，用 `jcmd <pid> VM.flags` 或 GC 日志确认 `-Xmx1024m`。
3. **Prometheus/Grafana/Alertmanager 未纳入本次运行**：只起了 postgres/redis/kafka。指标暴露已 curl 验证，但抓取快照、`etl_pipeline_hotpath.json` 面板渲染、`alerts.yml` 告警触发未端到端验证。复现：`docker compose up -d prometheus grafana alertmanager node-exporter` 后核对 `http://localhost:9090/targets` 与 Grafana 面板。
4. **幂等去重 / DLT / checkpoint / 断点续跑**属 Phase 1+，本次未涉及（正常，Phase 0 范围外）。
5. `demo_people` 与各 `demo_people_<taskId>` 目标表、若干数据源记录遗留在测试库；`e2e-logs/` 为运行产物（含日志/脚本）。

## 6. 清理（可选）

```bash
docker exec postgres psql -U postgres -d etl_system -c "DROP TABLE IF EXISTS demo_people;"
# 删除测试目标表：DROP TABLE demo_people_42d3921d; 等
# 停止服务：按端口反查 PID 杀掉；或 docker compose down 停 infra
docker compose stop postgres redis kafka
```

## 7. 一键化脚本（本次实际使用，位于 e2e-logs/）

- `launch-stack.ps1` / `relaunch2.ps1` / `final.ps1`：按端口健康门控启动服务并记录 `status*.txt`。
- `redrive.ps1` / `redrive2.ps1`：重建改动模块 → 重启 config-server/transformation → 建任务 → 轮询终态 → 校验目标表/指标（`redrive2-result.txt` 为最终绿色证据）。
- `seed.sql`：样例源表建表与数据。

> 复用建议：正式 runbook 应把"按端口杀进程 → clean package → 依序启动 → 驱动 → 校验"固化为一个脚本，避免本会话中反复出现的 jar 锁竞态。

## 8. 关联文档

- 演进方案：`etl-architecture-evolution-plan.md`；问题清单：`architecture-overview.md §5` + `risk-addendum-and-tech-selection.md`（H~M）。
- 性能基线：`performance-analysis.md`（§6.5 记 Phase 0 落地）。
- 图：`target-*.dot`、`risk-tech-selection-matrix.dot`。

## 9. 复验补记（①监控栈 ②trace 日志 ③清理）

### 9.1 ② trace 日志（已修 + 复验）
- 根因：pipeline 服务各自带 `logback-spring.xml`，**存在自定义 logback 时 `logging.pattern.*` 属性被完全忽略**（此前 Phase 0 把 pattern 放 config-repo/本地 yml 均无效）。日志实际渲染为 `logback-spring.xml` 里硬编码的无毫秒、无 trace 旧格式。
- 修复：改 4 个流水线服务 `logback-spring.xml` 的 `<pattern>`，加入 `[trace:%X{traceId:-},span:%X{spanId:-},task:%X{taskId:-}]` + 毫秒；并**回退** config-repo 与本地 application.yml 里失效的 `logging.pattern` dead config（避免双源误导）。
- 复验（`e2e-logs/verify-trace.txt`，task `67ffd4f2`）：storage 日志现为
  `... INFO  [trace:,span:,task:67ffd4f2-...] c.l.d.d.service.DataStorageService - All records completed for task 67ffd4f2 ...`
  → **业务 taskId 关联生效 ✅**，日志格式统一 ✅。
- 残留发现：`trace/span` 仍为空——Sleuth 3.1 不插桩本项目**手写的自定义 `ConcurrentKafkaListenerContainerFactory`**，跨服务 trace 上下文未建立。彻底打通 trace 需 Phase 1（编排器接管、用 Boot/Sleuth 自动插桩的工厂）或显式接入 `TracingKafkaConsumer/Producer`。当前以 taskId 作为业务关联主键（可接受）。

### 9.2 ① 监控栈（已起 + 核验）
`docker compose up -d prometheus grafana alertmanager node-exporter alert-webhook` 后，`e2e-logs/verify-monitoring.txt`：
- Prometheus healthy；**`spring-actuator` 抓取目标 6/6 全 up**（8081-8085 + 8761，`host.docker.internal` 生效）。
- **`etl_records_in_total=8`、`etl_records_out_total=8` 已被 Prometheus 实际采集**（闭合 `performance-analysis.md §1` 记录的“监控配了但没落地”缺口）；`jvm_gc_pause_seconds_count` 12 series。
- 告警规则加载：`JvmFullGcStorm, EtlLoadSlowRegression, EtlRecordsDroppedGrowing, EtlInsertRetryHigh, EtlConsumerLagHigh`（+原有 4 条）。
- Grafana 已 provision **“ETL Pipeline Hotpath (Phase 0)”** 面板（另有 ETL System Overview / JVM 系列）；Grafana health db=ok。
- **既有缺陷（非 Phase 0，已修复）**：原 Alertmanager 在 `9095` 不可达——compose `ports: 9095:9095`，但 Alertmanager 默认监听 **9093**；且 `prometheus.yml` 的 `alerting.targets: alertmanager:9095` 随之错配 → 告警发不出。
- **修复**（统一走容器默认 9093）：`docker-compose.yml` alertmanager 改为 `ports: 9095:9093` + 显式 `command: --config.file=/etc/alertmanager/config.yml --web.listen-address=:9093`；`prometheus.yml` 的 alerting 目标改 `alertmanager:9093`。
- **复验**（`e2e-logs/verify-alertmanager.txt`，重建后）：宿主 `http://localhost:9095/-/ready` => **HTTP 200**；Prometheus `/api/v1/alertmanagers` **activeAlertmanagers=`http://alertmanager:9093/api/v2/alerts`**（dropped 为空）；Alertmanager `/api/v2/status` 已加载 `route.receiver: webhook`。告警链路 Prometheus→Alertmanager→alert-webhook 打通。

### 9.3 ③ 清理
- `.gitignore` 第 46 行**已含 `e2e-logs/*`**（此前已配置，无需再加）。
- 已停本次启动的 Java 服务、删除测试数据（`demo_people`、各 `demo_people_<taskId>` 目标表、e2e 数据源行）。infra/监控容器可按需 `docker compose down` 停止。
- 注：`docs/` 已不再被 `.gitignore` 整体忽略（仅 `e2e-logs/*` 在库外）；本 runbook 与 `docs/architecture/*` 应随代码一并提交。

## 10. Phase 1B 灰度回滚 runbook（Gate 2 演练项）

> 触发条件：灰度期出现 P0 信号——shadow compare 持续不一致、`NEEDS_ATTENTION` 激增、孤儿 run、目标表重复写入、
> 或 `/progress` 显示分片长时间无心跳。原则：**只切路由与取消，不手改状态、不删 checkpoint**。

1. **冻结新链路提交**：把 `config-server/src/main/resources/config-repo/task-management-service.yml` 的
   `etl.routing.new-worker` 改为 `false`（`shadow-compare` 可保留为 `true` 观察），刷新/重启 task-management。
   确认 `DispatchService.pump()` 直接返回、`isNewWorkerFor()` 全部为 `false`；此时旧链路 `TaskScheduler` 恢复独占。
2. **处理在途 run**：对每个非终态 run 调 `POST /api/tasks/{taskId}/cancel`；等待 worker ack，超 90s 由
   `StallAndCancelSweeper` 走 T16 强制取消。**禁止** `UPDATE etl_task_run SET status=...` 或改 `etl_task.status`。
3. **确认排空**：`GET /api/tasks/{taskId}/progress` 与 `/failures` 确认无新增 `ASSIGNED`；需要续跑时只在旧链路恢复后
   重新提交，或对新链路 run 用 `POST /resume`（会经 `__reclaimed__` 回收 lease）。
4. **旧链路接管**：保持旧 worker/旧 topic 消费组在线；新 worker 实例可停，未确认终态的 run 由 cancel 收口。
5. **回滚后校验**（四条 SQL 均返空才算通过）：

```sql
-- ① 孤儿 run：非终态且无 ASSIGNED/RUNNING 分片、超过 5 分钟无进度
SELECT r.run_id, r.status, r.last_progress_ts FROM etl_task_run r
 WHERE r.is_current AND r.status NOT IN ('SUCCEEDED','FAILED','CANCELLED','SUPERSEDED')
   AND NOT EXISTS (SELECT 1 FROM etl_task_checkpoint c
                    WHERE c.run_id=r.run_id AND c.status IN ('ASSIGNED','RUNNING'))
   AND (r.last_progress_ts IS NULL OR r.last_progress_ts < now() - interval '5 minutes');

-- ② 重复写入：目标表 idempotency_key 必须唯一（UNIQUE 索引存在时理论为空，作回归探测）
--    SELECT idempotency_key, COUNT(*) FROM <target_table> GROUP BY 1 HAVING COUNT(*) > 1;

-- ③ 计数平衡（G-4 / §12.2 校验 2）与 ④ 多非终态 run（INV-1）见 §12.2 原 SQL
```

6. **留证**：记录 taskId/runId 时间线、`etl_run_state_count`、`etl_checkpoint_lag_seconds`、`etl_dlt_unresolved_size`、
   shadow 差异样本；回滚演练报告归档后再评估是否重新开启 `etl.routing.new-worker`。
