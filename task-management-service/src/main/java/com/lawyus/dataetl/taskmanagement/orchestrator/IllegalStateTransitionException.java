package com.lawyus.dataetl.taskmanagement.orchestrator;

/**
 * 非法状态迁移（迁移表 G-1 之外的 from→to）。
 *
 * <p>与 CAS 失败严格区分（{@code orchestrator-design.md §6.1 契约要点 #2}）：
 * CAS 失败 = 并发/重复投递，幂等忽略、不抛异常；本异常 = 协议被破坏，必须可见。
 * 消费侧捕获后按 §6.4 落 DLT 并 ack（绝不重试，防 poison loop），并计入
 * {@code etl.transition.illegal.total}。
 */
public class IllegalStateTransitionException extends RuntimeException {

    private final EtlTaskStatus from;
    private final EtlTaskStatus to;

    public IllegalStateTransitionException(EtlTaskStatus from, EtlTaskStatus to) {
        super("Illegal ETL run status transition: " + from + " -> " + to);
        this.from = from;
        this.to = to;
    }

    public EtlTaskStatus getFrom() {
        return from;
    }

    public EtlTaskStatus getTo() {
        return to;
    }
}
