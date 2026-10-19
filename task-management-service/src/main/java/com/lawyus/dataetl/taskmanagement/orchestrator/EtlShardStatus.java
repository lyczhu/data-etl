package com.lawyus.dataetl.taskmanagement.orchestrator;

/**
 * checkpoint（分片）子状态机的状态集合（6 个）。
 *
 * <p>权威定义与迁移见 {@code docs/architecture/orchestrator-design.md §2.1 / §4.3}。
 * 分片由 worker 直写（写权矩阵 §4.1），此枚举仅描述单个 shard 的断点进度。
 *
 * <p>注意：{@code CANCELLED} 在 shard 层不存在——取消是 run 级意图，
 * 落 checkpoint 时保留原状态并置 {@code etl_task_run.cancel_requested_at}（D-18）。
 */
public enum EtlShardStatus {

    /** 分片计划已生成，尚未派出。 */
    PENDING,
    /** dispatch 消息已发，等待 worker 领取。 */
    ASSIGNED,
    /** worker 已领取并开始处理 chunk。 */
    RUNNING,
    /** 游标越界且目标表写入提交成功。 */
    SUCCEEDED,
    /** 分片重试耗尽或致命错误。 */
    FAILED,
    /** 人工跳过（毒分片）。 */
    SKIPPED;

    /** 是否属于"仍在跑/占用 worker"的状态（quiesce 判定的依据，G-3）。 */
    public boolean isActive() {
        return this == ASSIGNED || this == RUNNING;
    }
}
