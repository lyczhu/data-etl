package com.lawyus.dataetl.taskmanagement.orchestrator;

/**
 * 错误三分类，决定失败处置路径（见 {@code docs/architecture/orchestrator-design.md §7.4}）。
 * 落库为 {@code etl_task_checkpoint.error_class}（VARCHAR(24)）与 audit 记录。
 *
 * <ul>
 *   <li>{@link #TRANSIENT}：网络 / 锁 / 超时 → 分片内指数退避 + 抖动，最多 3 次。</li>
 *   <li>{@link #PERSISTENT}：持续性错误 → 任务级重试 {@code attempt_no += 1}，退避 5m/15m/1h。</li>
 *   <li>{@link #FATAL}：schema 不匹配 / 脏数据 → 立即 DLT，不重试。</li>
 *   <li>{@link #FATAL_CONFIG}：任务定义不合法（源表不存在、凭据失败）→ run 直接 FAILED。</li>
 *   <li>{@link #ILLEGAL_TRANSITION}：协议被破坏（迁移表外的 transition 调用）→ 进 DLT、绝不重试（P5）。</li>
 * </ul>
 */
public enum ErrorClass {
    TRANSIENT,
    PERSISTENT,
    FATAL,
    FATAL_CONFIG,
    ILLEGAL_TRANSITION
}
