package com.lawyus.dataetl.taskmanagement.orchestrator.event;

/**
 * 领域事件外发抽象。业务代码（{@code OrchestratorService}）只依赖本接口，
 * 从而在 Phase 1（{@code @TransactionalEventListener} 直投 Kafka）与 Phase 2（outbox 表 + poller）
 * 之间切换实现而无需改动调用方（{@code orchestrator-design.md §6.3}）。
 */
public interface DomainEventPublisher {

    /** 在当前事务提交后外发事件；事务回滚则不发送。 */
    void publishAfterCommit(Object event);
}
