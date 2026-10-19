package com.lawyus.dataetl.taskmanagement.orchestrator;

import java.util.Collections;
import java.util.Date;
import java.util.List;

/**
 * {@link OrchestratorService#transition} 的上下文，承载迁移的元信息与 guard 求值所需数据。
 *
 * <p>字段对应 {@code orchestrator-design.md §6.1}：{@code reason, actor, errorClass, shardIds, forced}；
 * 另含 {@code taskId}（供 INV-6 派生刷新）与 {@code now}（供可注入 Clock 的退避/超时判定，§12.1）。
 *
 * <p>{@code actor} 只从服务端 JWT principal 取，<b>绝不接受客户端传入</b>（§9）。
 */
public final class TransitionContext {

    private final String taskId;
    private final String reason;
    private final String actor;
    private final ErrorClass errorClass;
    private final List<String> shardIds;
    private final boolean forced;
    private final Date now;
    private final int maxAttempts;

    private TransitionContext(Builder b) {
        this.taskId = b.taskId;
        this.reason = b.reason;
        this.actor = b.actor;
        this.errorClass = b.errorClass;
        this.shardIds = b.shardIds == null ? Collections.emptyList() : Collections.unmodifiableList(b.shardIds);
        this.forced = b.forced;
        this.now = b.now == null ? new Date() : b.now;
        this.maxAttempts = b.maxAttempts;
    }

    public static Builder builder(String runIdTaskId) {
        return new Builder().taskId(runIdTaskId);
    }

    public String taskId() {
        return taskId;
    }

    public String reason() {
        return reason;
    }

    public String actor() {
        return actor;
    }

    public ErrorClass errorClass() {
        return errorClass;
    }

    public List<String> shardIds() {
        return shardIds;
    }

    public boolean forced() {
        return forced;
    }

    public Date now() {
        return now;
    }

    public int maxAttempts() {
        return maxAttempts;
    }

    /** 便于 guard 失败/审计时回填 reason。返回浅拷贝。 */
    public TransitionContext withReason(String newReason) {
        return toBuilder().reason(newReason).build();
    }

    public Builder toBuilder() {
        return new Builder()
                .taskId(taskId)
                .reason(reason)
                .actor(actor)
                .errorClass(errorClass)
                .shardIds(shardIds)
                .forced(forced)
                .now(now)
                .maxAttempts(maxAttempts);
    }

    public static final class Builder {
        private String taskId;
        private String reason;
        private String actor;
        private ErrorClass errorClass;
        private List<String> shardIds;
        private boolean forced;
        private Date now;
        private int maxAttempts = 3;

        public Builder taskId(String taskId) {
            this.taskId = taskId;
            return this;
        }

        public Builder reason(String reason) {
            this.reason = reason;
            return this;
        }

        public Builder actor(String actor) {
            this.actor = actor;
            return this;
        }

        public Builder errorClass(ErrorClass errorClass) {
            this.errorClass = errorClass;
            return this;
        }

        public Builder shardIds(List<String> shardIds) {
            this.shardIds = shardIds;
            return this;
        }

        public Builder forced(boolean forced) {
            this.forced = forced;
            return this;
        }

        public Builder now(Date now) {
            this.now = now;
            return this;
        }

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = maxAttempts;
            return this;
        }

        public TransitionContext build() {
            return new TransitionContext(this);
        }
    }
}
