package com.lawyus.dataetl.taskmanagement.orchestrator.audit;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lawyus.dataetl.taskmanagement.orchestrator.EtlTaskStatus;
import com.lawyus.dataetl.taskmanagement.orchestrator.TransitionContext;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskAudit;
import com.lawyus.dataetl.taskmanagement.orchestrator.entity.EtlTaskRun;
import com.lawyus.dataetl.taskmanagement.orchestrator.repository.EtlTaskAuditRepository;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.stereotype.Component;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 状态迁移审计写入器（Phase 2B / INV-9 / P7）。
 *
 * <p>由 {@code OrchestratorService} 在状态迁移的<b>同一事务</b>内调用：
 * 审计写失败会回滚状态迁移，这是有意为之——没有审计的状态变更不可接受。
 * 审计只记录“已落地”的迁移；CAS 失败、非法迁移、guard 拦截不调用本类。
 */
@Component
public class AuditWriter {

    private final EtlTaskAuditRepository repository;
    private final MeterRegistry meterRegistry;
    private final ObjectMapper objectMapper;

    public AuditWriter(EtlTaskAuditRepository repository,
                       MeterRegistry meterRegistry,
                       ObjectMapper objectMapper) {
        this.repository = repository;
        this.meterRegistry = meterRegistry;
        this.objectMapper = objectMapper;
    }

    /** 新建 run：{@code (none) -> CREATED}。 */
    public void recordCreated(EtlTaskRun run, String actor) {
        write(EtlTaskAudit.record(run.getTaskId(), run.getRunId(), EtlTaskAudit.NONE,
                EtlTaskStatus.CREATED.name(), actor, "CREATE_RUN", null, null));
    }

    /** 一次成功落地的状态迁移。 */
    public void record(EtlTaskRun run, EtlTaskStatus from, EtlTaskStatus to, TransitionContext ctx) {
        String context = contextJson(ctx);
        write(EtlTaskAudit.record(run.getTaskId(), run.getRunId(), from.name(), to.name(),
                ctx.actor(), ctx.reason(),
                ctx.errorClass() == null ? null : ctx.errorClass().name(), context));
    }

    private void write(EtlTaskAudit audit) {
        try {
            repository.save(audit);
        } catch (RuntimeException e) {
            meterRegistry.counter("etl.audit.write.failures").increment();
            throw e;
        }
    }

    /** 上下文 JSON 快照；序列化失败不影响迁移本身（context 置空）。 */
    private String contextJson(TransitionContext ctx) {
        if (ctx == null) {
            return null;
        }
        Map<String, Object> context = new LinkedHashMap<>();
        context.put("reason", ctx.reason());
        context.put("actor", ctx.actor());
        context.put("errorClass", ctx.errorClass() == null ? null : ctx.errorClass().name());
        context.put("shardIds", ctx.shardIds());
        context.put("forced", ctx.forced());
        context.put("maxAttempts", ctx.maxAttempts());
        context.put("now", ctx.now() == null ? null : ctx.now().getTime());
        try {
            return objectMapper.writeValueAsString(context);
        } catch (RuntimeException | com.fasterxml.jackson.core.JsonProcessingException e) {
            return null;
        }
    }
}
