# 主流数据接入系统的任务状态判定方式（外部参照材料）

> **文档定位**：本文只描述**外部产品的公开机制**，用于理解编排类系统状态语义的由来、回答"为什么不直接采用 Flink / SeaTunnel / Airflow"，**不构成对本仓库实现的承诺或约束**，也不纳入任何评审基线。
> **本项目对应实现**（不在此文范围内重复）：`orchestrator-design.md` §1 设计原则 P1–P8、§4.1 写权矩阵、§7.5 收敛判定与行数对账；选型对照决策见 `risk-addendum-and-tech-selection.md §2`。
> **查阅时间**：2026-10。参照对象与代际：Apache Kafka 3.x（Kafka Connect）、Apache Flink 1.18+、Apache SeaTunnel 2.3.x（Zeta 引擎）、Apache NiFi 1.x/2.x、Apache DolphinScheduler 3.2.x、Apache Airflow 2.x/3.x、Airbyte（Platform 架构）、DataX 3.x、Informatica EIC / IBM DataStage Orchestrate（商业闭源，仅据公开文档与产品说明推断）。外部机制随版本演进，**引用本文结论对外沟通前请复核当期文档**。

---

## 1. 核心结论

**任务状态之所以可被管好，根因不是"把 E/T/L 收进同一个进程"。**真正的三支承重柱是：

1. **状态迁移权唯一归属**——每个状态行只有一个写入者，其余角色只能"请求迁移"；
2. **进度证据外部化且可重算**——完成与否由持久化的 checkpoint/offset 聚合重算得出，不由执行者自报；
3. **活性靠租约 + 心跳 + 周期性清扫器判定**——没有例外，只有实现载体不同（ZooKeeper / DB / Kafka group）。

进程收缩是让这三支**更容易做对**的手段（它减少需要判定的中间态数量），而不是这三支本身的替代品。

**直接反例**：Airflow、DolphinScheduler、Informatica、DataStage Orchestrate 都**刻意不收缩**——一个 task 就是别的节点上的黑盒进程/容器，调度器只看到退出码与心跳，状态管理依然成立。

---

## 2. 三个流派

| 流派 | 代表产品 | E/T/L 是否同进程 | "是否结束"怎么判 | "是否还活着"怎么判 |
|---|---|---|---|---|
| **引擎类**（数据面闭环） | Flink、Spark、SeaTunnel Zeta、Kafka Connect、NiFi、DataX | **是**，算子链 / task 线程内串联 | 全局 checkpoint barrier + `notifyCheckpointComplete` / sink 侧 2PC 预提交；"完成"是一个**有协调者参与的一致性时刻** | JobMaster↔TaskManager 心跳 + Slot 租约；失联即整作业 failover，从最近 checkpoint 重启 |
| **编排类**（数据面黑盒） | Airflow、DolphinScheduler、Argo Workflows、Kestra、dbt + 外部调度、Informatica EIC、DataStage | **否**，task = subprocess / Pod / agent job | **不判数据语义**，只判进程终态；数据对账外包给 SQL assertion / 数据质量组件 | task heartbeat 写 DB 或 ZK watcher；超时 → `NEED_FAULT_TOLERANCE` 一类容错态后重派 |
| **混合类** | Airbyte（source/destination 分离容器 + 编排层）、StreamSets DC/Transformer + Control Hub、AWS Glue + Step Functions、Flink CDC + Session Operator | **拆**，动机是故障域 / 语言隔离 / 安全边界 | source 发出 STATE 记录 → 平台持久化，destination ack 后才推进 cursor | 编排层持有 workflow lease（Airbyte 平台架构用 Temporal），agent 上报 + 心跳 |

### 2.1 引擎类的内部分野：有没有全局 barrier

同为"数据面同进程"，判定强度差一个量级，分水岭是**是否存在协调者发起的全局快照**：

- **有 barrier**：Flink（CheckpointCoordinator + 算子链对齐）、SeaTunnel Zeta（`triggerCheckpoint` 分发 barrier，状态以 Hazelcast IMap 为一级缓存、异步落 HDFS/S3，Sink 侧配合 2PC 才可宣称精确一次）。
- **无 barrier，只有分段位点**：Kafka Connect——offset 按间隔提交（默认 10s），任务失败从上次已提交位点重放，官方明确**只提供 at-least-once**；NiFi——FlowFile 走 WAL 存储库 + 处理器级 `StateManager`（Local/Cluster 两种 scope，集群态存 ZooKeeper 且整个 State Map 序列化后须 < 1 MB，超出就得自建外部存储）；DataX——单进程内 Reader→Channel→Writer，状态判定实际外包给拉起它的调度方。

### 2.2 编排类的共同难题：僵尸任务

编排类把状态写进 DB，于是状态与进程真实存亡可能背离。DolphinScheduler 的典型成因：Worker 假死（长 Full GC）或网络抖动导致 `t_ds_task_instance.state` 停在 RUNNING，但进程早已消失；或任务写成功后、状态上报前失败。其对策是把清扫逻辑做成判定树：基于 ZooKeeper remove 事件触发容错，**只容错任务实例**，并比较实例开始时间与服务节点启动时间——实例若晚于节点启动则跳过容错，避免误杀刚接手的任务。这是 sweeper 的通用原型：**租约超时 + 时间基准比较 + 幂等重派**，三者缺一不可。

---

## 3. 跨流派共有的 5 条不变式

1. **一个状态行只有一个 writer**。Kafka Connect 中 task 状态写入 `status.storage.topic` 由 herder 收敛，REST 状态（`UNASSIGNED/RUNNING/PAUSED/STOPPED/FAILED/RESTARTING`）从该主题读，因此天然存在可见延迟；DolphinScheduler 只有 Master 推进 WorkflowInstance；Flink 只有 JobMaster 改 ExecutionGraph。worker 永远只能"请求迁移"。
2. **完成判定基于可重算的证据，不基于自报计数**。Flink 用 checkpoint metadata 聚合；Connect 用 offset store；DolphinScheduler 的 Workflow 状态是所有 Task 实例状态的函数——其官方表述是"Workflow 不主动执行，只根据 Task 状态变化推进"。自报数字只用于进度展示。
3. **活性 = 超时 + 清扫器**。所有产品都实现为"租约/心跳 + 一个周期性判定树"，区别仅在租约放 ZK、DB 还是 Kafka group。Connect 的心跳默认 3s，故节点失效通常数秒内被识别。
4. **attempt 是状态机的一等公民**。Flink 有 restart strategy 与 execution attempt，DolphinScheduler 有任务实例重试与容错态，Connect 有 `RESTARTING`。没有 attempt 概念就无法区分"重派"与"重复执行"，重试语义必然含糊。
5. **Exactly-once 只在有全局 barrier + sink 2PC 的引擎里存在**。跨进程又想要 exactly-once，代价就是引入预提交/提交两阶段与一个更强的协调者，没有第三条路。其余产品的公开承诺都是 at-least-once + 幂等（去重靠目标侧主键/upsert 或物化视图）。

---

## 4. 为什么数据面确实普遍向同进程收敛

**每跨一次进程边界，就多一组必须判定的中间态和至少一个 sweeper。**

E→T→L 拆成三个服务时，以下都是独立可观测状态：`E 完成但 T 未消费`、`T 完成但 L 落库失败`、`L 落库成功但回执丢失`。收进一个进程后，它们塌缩成"本地事务未提交"这一种，恢复语义从"跨服务对账"降为"回滚重来 + 幂等"。

因此准确的说法是：**进程收缩降低的是状态数量，不是管理状态的方法。**用微服务拆分换来弹性，就要用协调者 + 持久化状态 + 清扫器把丢掉的可判定性补回去——这笔账在引擎类（barrier 补）和编排类（DB 状态 + failover 补）里都能算清。

---

## 5. 主流产品刻意拆进程的场景与补偿

拆进程并非不懂状态管理，而是有别的约束压过来：

| 拆分动机 | 代表 | 补偿机制 |
|---|---|---|
| 故障域与崩溃隔离（插件是第三方代码） | Airbyte source/destination 分离容器；Kafka Connect 独立 worker 进程 | 平台层集中持有 STATE/offset，destination ack 后才推进 cursor；编排层用 lease 兜活性 |
| 语言与运行时隔离（Python/Java/Go 混编） | Airbyte、部分商业 ETL 的 agent 模型 | 协议化消息（stdout + 结构化记录）+ 单点状态持久化 |
| 计算弹性伸缩 | Flink/Spark 的 TaskManager/Executor 水平扩 | 全局 checkpoint + 外部 state backend（RocksDB / 对象存储），状态与计算彻底解耦 |
| 多租户安全边界 | 云上托管服务（Glue、Dataflow 等） | 作业状态由控制面服务持有，引擎侧只上报终态；用户可见状态由控制面裁决 |

共同点：**一旦拆开，就必须把"可重算的进度证据"和"活性租约"显式建出来**。留在同进程里的产品则省掉这套机制，代价是伸缩性与隔离性上限。

---

## 6. 主要参考出处

- Apache Kafka — Kafka Connect Administration / Connector Development Guide（内部主题、状态机、rebalance、offset 提交语义）：<https://kafka.apache.org/36/kafka-connect/administration/>
- Apache NiFi — Overview / Nifi In Depth / Administration Guide（Zero-Master Clustering、FlowFile WAL、State Management scope 与 1 MB 限制）：<https://nifi.apache.org/docs/nifi-docs/html/overview.html>
- Apache SeaTunnel — 官方 Blog（Zeta 引擎 JobMaster/PhysicalPlan/SubPlan 提交链路、checkpoint 与 2PC、同程旅行批流一体实践）：<https://seatunnel.apache.org/zh-CN/blog/>
- Apache DolphinScheduler — 官方博客《状态机：调度系统真正的灵魂》《"僵尸任务"怎么处理》（DB 为唯一事实源、Master/Worker failover、容错判定树）：<https://www.cnblogs.com/DolphinScheduler/>
- Apache Flink — Task/ExecutionGraph 双层状态、CheckpointCoordinator 与 `withIdleness`（idle source 的 watermark 处理）：Flink 官方 docs 与运行时架构篇。
