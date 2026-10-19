package com.lawyus.dataetl.connector.spi.model;

/**
 * 单行写入失败的原文快照（WP-04/WP-05）：由 sink 逐行降级时产出，worker 同事务写
 * {@code etl_task_dlt}，并据此阻止 run 进入 {@code SUCCEEDED}。
 *
 * @param sourceKey 失败行的源主键（JDBC 为 pk；用于把续跑游标回退到失败行之前）
 */
public record FailedRow(String errorClass, String errorMessage, String rawPayload, String sourceKey) {

    public FailedRow(String errorClass, String errorMessage, String rawPayload) {
        this(errorClass, errorMessage, rawPayload, null);
    }
}
