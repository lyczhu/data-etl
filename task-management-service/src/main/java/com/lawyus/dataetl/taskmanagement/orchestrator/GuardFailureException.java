package com.lawyus.dataetl.taskmanagement.orchestrator;

/**
 * 守护规则（G-2..G-7）求值失败。
 *
 * <p>迁移边本身合法（G-1 通过），但该次迁移的前置条件不满足——例如 quiesce 不成立（G-3）、
 * 行数对账不平（G-4 / BALANCE）、attempt 预算耗尽（G-5）。调用方据此选择降级路径
 * （如 G-4 不平应改走 {@code RUNNING -> NEEDS_ATTENTION} 而非 {@code SUCCEEDED}）。
 */
public class GuardFailureException extends RuntimeException {

    private final String guard;

    public GuardFailureException(String guard, String message) {
        super("Guard " + guard + " failed: " + message);
        this.guard = guard;
    }

    public String getGuard() {
        return guard;
    }
}
