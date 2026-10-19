package com.lawyus.dataetl.taskmanagement.orchestrator.event;

import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Component;

/**
 * Phase 1 的 {@link DomainEventPublisher} 实现：借助 Spring {@code @TransactionalEventListener} 语义，
 * 在 {@code transition()} 事务提交后触发外发（{@code orchestrator-design.md §6.3}）。
 *
 * <p>这里只负责把事件交给 Spring 事件机制；真正的 Kafka 投递（{@code etl.task.dispatch/progress/results}）
 * 在接线切片（§13 交付物 #8）注册 {@code AFTER_COMMIT} 监听器完成，本内核不触碰 Kafka。
 */
@Component
public class SpringDomainEventPublisher implements DomainEventPublisher {

    private final ApplicationEventPublisher publisher;

    public SpringDomainEventPublisher(ApplicationEventPublisher publisher) {
        this.publisher = publisher;
    }

    @Override
    public void publishAfterCommit(Object event) {
        // 若处于事务中，由 @TransactionalEventListener(AFTER_COMMIT) 在提交后消费；
        // 无事务上下文时监听器默认不触发（fallback 行为由注册侧 @TransactionalEventListener fallbackPhase 决定）。
        publisher.publishEvent(event);
    }
}
