package com.lawyus.dataetl.taskmanagement.orchestrator;

/**
 * ETL run 层状态机的状态集合（12 个），取代散落的历史字面量
 * （{@code new/processing/running/completed/failed}）。
 *
 * <p>权威定义见 {@code docs/architecture/orchestrator-design.md §2.1 / §3}。
 * 状态机的宿主是 {@code etl_task_run}，{@code etl_task.status} 只是 current run 的只读投影
 * （INV-6），因此本枚举描述的是 <b>run</b> 的生命周期。
 *
 * <p>终态（{@link #isTerminal()}）为 {@code SUCCEEDED / FAILED / CANCELLED / SUPERSEDED}：
 * 一旦进入不可回退，只有终态可被同 task 的新 run 取代为 {@link #SUPERSEDED}（禁止清单 #1）。
 */
public enum EtlTaskStatus {

    /** run 记录已落库，任务定义快照固化，尚未校验。 */
    CREATED,
    /** 校验链 V-1..V-4 全部通过。 */
    VALIDATED,
    /** 等待派工（{@code queued_reason} 区分 SCHEDULED / THROTTLED）。 */
    QUEUED,
    /** 已发出 ≥1 条 shard 派工消息，但尚未收到任何 worker 心跳。 */
    DISPATCHED,
    /** 至少一个 shard 有 worker 心跳确认，run 正在执行。 */
    RUNNING,
    /** 收敛时存在 FAILED 分片且其余为 SUCCEEDED/SKIPPED，已提交数据保留。 */
    PARTIAL_SUCCESS,
    /** 已决定任务级重试，处于退避窗口内，无 shard 在跑（quiesce 已确认）。 */
    RETRYING,
    /** 需人工介入：attempt 耗尽 / DLT 非空 / 卡死 / 饥饿 / 人工暂停。 */
    NEEDS_ATTENTION,
    /** 全部分片成功且行数对账平衡。 */
    SUCCEEDED,
    /** 系统判定不可能成功，或人工放弃。 */
    FAILED,
    /** 用户主动取消且已停止，已提交数据与 checkpoint 保留。 */
    CANCELLED,
    /** 仅 run 层存在：该终态 run 被同 task 的新 run 取代。 */
    SUPERSEDED;

    /** 终态：不可回退，只可被取代为 {@link #SUPERSEDED}。 */
    public boolean isTerminal() {
        return this == SUCCEEDED || this == FAILED || this == CANCELLED || this == SUPERSEDED;
    }

    /**
     * 兼容解析：把历史字面量（{@code new/processing/running/completed/failed}）与
     * 目标枚举名统一解析为 {@link EtlTaskStatus}，无法识别时抛异常。
     * 供适配层与 {@code refreshDerivedStatus} 读取旧数据时使用。
     */
    public static EtlTaskStatus fromLegacyOrName(String raw) {
        if (raw == null) {
            throw new IllegalArgumentException("status must not be null");
        }
        for (EtlTaskStatus s : values()) {
            if (s.name().equalsIgnoreCase(raw)) {
                return s;
            }
        }
        switch (raw.toLowerCase()) {
            case "new":
                return CREATED;
            case "running":
                return DISPATCHED;
            case "completed":
                return SUCCEEDED;
            case "failed":
                return FAILED;
            default:
                throw new IllegalArgumentException("Unknown EtlTask status: " + raw);
        }
    }
}
