package com.lawyus.dataetl.taskmanagement.orchestrator.dispatch;

/**
 * 派工消息外发抽象（{@code etl.task.dispatch.batch}）。Phase 1 用 {@link KafkaDispatchPublisher} 直投；
 * Phase 2 切 {@code etl_outbox}（同事务落表 + poller 投递），业务代码不感知（{@code orchestrator-design.md §6.3}）。
 */
public interface DispatchPublisher {

    /** key 用 taskId 保证同任务分区有序（§6.3）。 */
    void publish(ShardDispatchCommand command);
}
