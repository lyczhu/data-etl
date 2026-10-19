package com.lawyus.dataetl.taskmanagement.orchestrator.event;

import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;

/**
 * 状态迁移领域事件，在 {@code transition()} 事务提交后发布（Phase 1 语义，§6.3）。
 *
 * <p>Phase 1 采用 {@code @TransactionalEventListener(AFTER_COMMIT)}：状态已落库后才外发事件；
 * 发送失败仅记 ERROR + 指标，正确性由 sweeper 与 §7.5 重算兜底（弱 at-least-once）。
 * Phase 2 换 {@code etl_outbox} 表 + poller 得到强保证，业务代码经 {@link DomainEventPublisher}
 * 接口不感知切换。
 */
public class EtlTaskTransitioned {

    private final String runId;
    private final String taskId;
    private final EtlTaskStatus from;
    private final EtlTaskStatus to;
    private final TransitionContext context;

    public EtlTaskTransitioned(String runId, String taskId, EtlTaskStatus from, EtlTaskStatus to,
                               TransitionContext context) {
        this.runId = runId;
        this.taskId = taskId;
        this.from = from;
        this.to = to;
        this.context = context;
    }

    public String runId() {
        return runId;
    }

    public String taskId() {
        return taskId;
    }

    public EtlTaskStatus from() {
        return from;
    }

    public EtlTaskStatus to() {
        return to;
    }

    public TransitionContext context() {
        return context;
    }
}
