package com.lawyus.dataetl.worker.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.worker.model.ShardDispatch;
import com.lawyus.dataetl.worker.pipeline.ShardExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * 分片派工监听器（{@code etl.task.dispatch.batch}）。一条消息 = 一个 shard，交 {@link ShardExecutor} 进程内跑 E→T→L。
 *
 * <p>Poison Guard：非法 JSON 记 WARN 后跳过（ack），不抛错致整批热循环（沿用 Phase 0 storage 经验）。
 * 分片自身失败由 executor 落 checkpoint FAILED 并回报，不外抛。
 */
@Component
public class DispatchBatchListener {

    private static final Logger log = LoggerFactory.getLogger(DispatchBatchListener.class);

    private final ObjectMapper objectMapper;
    private final ShardExecutor executor;

    public DispatchBatchListener(ObjectMapper objectMapper, ShardExecutor executor) {
        this.objectMapper = objectMapper;
        this.executor = executor;
    }

    @KafkaListener(topics = "${etl.topics.dispatch-batch:etl.task.dispatch.batch}",
            groupId = "${etl.worker.consumer-group:etl-worker}")
    public void onDispatch(String payload) {
        ShardDispatch dispatch;
        try {
            dispatch = objectMapper.readValue(payload, ShardDispatch.class);
        } catch (Exception parseFail) {
            log.warn("illegal dispatch payload, skip (poison guard): {}", parseFail.getMessage());
            return;   // ack 并跳过，绝不重试整批
        }
        // §6.1/B1：日志 MDC 带上 taskId/runId/shardId/attemptNo，按 taskId 可直查整条分片链路
        MDC.put("taskId", dispatch.getTaskId());
        MDC.put("runId", dispatch.getRunId());
        MDC.put("shardId", dispatch.getShardId());
        MDC.put("attemptNo", String.valueOf(dispatch.getAttemptNo()));
        try {
            executor.execute(dispatch);
        } catch (RuntimeException e) {
            // executor 已就 shard 失败落 checkpoint + 回报；此处兜底防监听线程崩溃
            log.error("unexpected dispatch execution error run={} shard={}: {}",
                    dispatch.getRunId(), dispatch.getShardId(), e.getMessage(), e);
        } finally {
            MDC.remove("taskId");
            MDC.remove("runId");
            MDC.remove("shardId");
            MDC.remove("attemptNo");
        }
    }
}
