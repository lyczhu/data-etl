package com.lawyus.dataetl.worker.pipeline;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 目标行幂等键（方案 v2 WP-03 / D2）：{@code sha256(taskId|sourceTable|pk)}。
 *
 * <p>键不包含 runId：同一 task 的重复 run/重放必须命中同一目标行，否则“重试不重复”不成立。
 * 对无主键的行退化为整行内容哈希（尽力而为；Phase 1A 仅 JDBC 主键源）。
 */
public final class IdempotencyKeys {

    public static final String COLUMN = "idempotency_key";

    private IdempotencyKeys() {
    }

    public static String of(String taskId, String sourceTable, Object pk) {
        String basis = taskId + "|" + (sourceTable == null ? "" : sourceTable) + "|" + String.valueOf(pk);
        return sha256Hex(basis);
    }

    /** 给记录追加 {@code idempotency_key} 列（不覆盖同名源列）。 */
    public static SourceRecord withKey(SourceRecord record, String key) {
        if (record.columns().containsKey(COLUMN)) {
            return record;
        }
        Map<String, Object> columns = new LinkedHashMap<>(record.columns());
        columns.put(COLUMN, key);
        return new SourceRecord(record.shardId(), record.pk(), columns);
    }

    static String sha256Hex(String value) {
        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            byte[] digest = md.digest(value.getBytes(StandardCharsets.UTF_8));
            StringBuilder sb = new StringBuilder(digest.length * 2);
            for (byte b : digest) {
                sb.append(Character.forDigit((b >> 4) & 0xF, 16));
                sb.append(Character.forDigit(b & 0xF, 16));
            }
            return sb.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }
}
