package com.lawyus.dataetl.worker.pipeline.transform;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;
import com.lawyus.dataetl.worker.pipeline.RecordTransformer;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 日期归一：把多种来源的日期/时间字符串统一为 ISO-8601（{@code yyyy-MM-dd} 或 {@code yyyy-MM-dd HH:mm:ss}）。
 * 面向 CSV/JSON/Mongo 里混乱的日期写法（斜杠、多种分隔符、epoch 秒/毫秒）。
 *
 * <p>纯函数、无副作用；无法识别的值原样保留（不误伤普通文本）。<b>应在 {@link TypeCoercionTransformer} 之前跑</b>，
 * 否则 epoch 数字串会被先强制成 Long 而漏判。
 */
public class DateNormalizeTransformer implements RecordTransformer {

    private static final Pattern EPOCH_MS = Pattern.compile("\\d{13}");
    private static final Pattern EPOCH_S = Pattern.compile("\\d{10}");

    private static final DateTimeFormatter OUT_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter OUT_DATE_TIME = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /** 依次尝试的入参格式（含时间在前，避免日期格式吞掉带时间的串）。 */
    private static final List<DateTimeFormatter> DATE_TIME_FORMATS = List.of(
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss"),
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy HH:mm:ss"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy HH:mm:ss"));

    private static final List<DateTimeFormatter> DATE_FORMATS = List.of(
            DateTimeFormatter.ofPattern("yyyy-MM-dd"),
            DateTimeFormatter.ofPattern("yyyy/MM/dd"),
            DateTimeFormatter.ofPattern("dd-MM-yyyy"),
            DateTimeFormatter.ofPattern("dd/MM/yyyy"));

    @Override
    public SourceRecord apply(SourceRecord record) {
        if (record == null || record.columns() == null) {
            return record;
        }
        LinkedHashMap<String, Object> out = new LinkedHashMap<>();
        for (Map.Entry<String, Object> e : record.columns().entrySet()) {
            out.put(e.getKey(), normalize(e.getValue()));
        }
        return new SourceRecord(record.shardId(), record.pk(), out);
    }

    /** 可见以便单测。 */
    static Object normalize(Object v) {
        if (!(v instanceof String s) || s.isBlank()) {
            return v;
        }
        String t = s.trim();
        if (EPOCH_MS.matcher(t).matches()) {
            return LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.parseLong(t)), ZoneOffset.UTC)
                    .format(OUT_DATE_TIME);
        }
        if (EPOCH_S.matcher(t).matches()) {
            return LocalDateTime.ofInstant(Instant.ofEpochSecond(Long.parseLong(t)), ZoneOffset.UTC)
                    .format(OUT_DATE_TIME);
        }
        for (DateTimeFormatter f : DATE_TIME_FORMATS) {
            try {
                return LocalDateTime.parse(t, f).format(OUT_DATE_TIME);
            } catch (DateTimeParseException ignored) {
                // try next
            }
        }
        for (DateTimeFormatter f : DATE_FORMATS) {
            try {
                return LocalDate.parse(t, f).format(OUT_DATE);
            } catch (DateTimeParseException ignored) {
                // try next
            }
        }
        return v;
    }
}
