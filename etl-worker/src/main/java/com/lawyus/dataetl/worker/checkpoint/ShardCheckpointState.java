package com.lawyus.dataetl.worker.checkpoint;

/**
 * checkpoint 快照（worker 读侧）。{@code lastCursor} 是“已提交的最后一行主键”，
 * 重试时必须从它之后续读（开区间）。
 */
public record ShardCheckpointState(String status,
                                   int shardAttempt,
                                   String lastCursor,
                                   String lastPk,
                                   long rowsIn,
                                   long rowsOut,
                                   long rowsFiltered,
                                   long rowsFailed,
                                   long rowsDropped,
                                   long rowsDeduped) {

    public static ShardCheckpointState empty() {
        return new ShardCheckpointState(null, 0, null, null, 0, 0, 0, 0, 0, 0);
    }

    public boolean hasCursor() {
        return lastCursor != null && !lastCursor.isBlank();
    }
}
