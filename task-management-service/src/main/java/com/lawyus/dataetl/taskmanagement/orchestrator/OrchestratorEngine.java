package com.lawyus.dataetl.taskmanagement.orchestrator;

import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;

/**
 * 编排引擎抽象（{@code orchestrator-design.md §2.1 / §9.4}、补充件 §2.4）。
 *
 * <p>本设计的后门：当并发规模或跨节点 DAG 诉求上升、需要整体切换到 Temporal / Airflow 等
 * 外部编排实现时，只需替换本接口的实现，run 层三层状态模型与对账等式语义不受影响。
 *
 * <p>Phase 1 的唯一实现是自研的 {@link OrchestratorService}（手写状态表 + {@code @TransactionalEventListener}）。
 */
public interface OrchestratorEngine {

    /**
     * 执行一次受守护的 run 层状态迁移。实现必须保证：
     * <ul>
     *   <li>迁移表外的 from→to 抛 {@link IllegalStateTransitionException}（协议破坏，可见）；</li>
     *   <li>并发/重复投递以 CAS 实现幂等（返回真实状态，不抛异常）；</li>
     *   <li>状态字段是唯一的写路径，外部不得直接改 {@code etl_task_run.status}。</li>
     * </ul>
     */
    EtlTaskRun transition(String runId, EtlTaskStatus from, EtlTaskStatus to, TransitionContext ctx);
}
