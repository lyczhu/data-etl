package com.lawyus.dataetl.datastorage.config;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import org.springframework.stereotype.Component;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Phase 0 可观测性埋点：入库阶段（load）的核心指标，经 /actuator/prometheus 暴露。
 * 刻意不以 taskId 作标签（基数无界），仅用低基数标签 stage。
 */
@Component
public class EtlMetrics {

    private final Counter recordsIn;
    private final Counter recordsOut;
    private final Counter recordsDropped;
    private final Counter recordsDeduped;
    private final Counter insertRetry;
    private final Timer loadTimer;
    private final AtomicLong lastLoadMillis = new AtomicLong(0);

    public EtlMetrics(MeterRegistry registry) {
        this.recordsIn = Counter.builder("etl.records.in").tag("stage", "load")
                .description("入库阶段收到的记录数").register(registry);
        this.recordsOut = Counter.builder("etl.records.out").tag("stage", "load")
                .description("成功写入目标库的记录数").register(registry);
        this.recordsDropped = Counter.builder("etl.records.dropped").tag("stage", "load")
                .description("因消息非法被丢弃的记录数").register(registry);
        this.recordsDeduped = Counter.builder("etl.records.deduped").tag("stage", "load")
                .description("幂等去重命中的记录数").register(registry);
        this.insertRetry = Counter.builder("etl.insert.retry").tag("stage", "load")
                .description("批量插入重试次数").register(registry);
        this.loadTimer = Timer.builder("etl.load.duration")
                .description("单任务批次入库耗时").register(registry);
    }

    public void recordIn(long n) { recordsIn.increment(n); }
    public void recordOut(long n) { recordsOut.increment(n); }
    public void recordDropped() { recordsDropped.increment(); }
    public void recordDeduped() { recordsDeduped.increment(); }
    public void recordInsertRetry() { insertRetry.increment(); }

    public void recordLoadDuration(Duration duration) {
        loadTimer.record(duration);
        lastLoadMillis.set(duration.toMillis());
    }

    public long getLastLoadMillis() {
        return lastLoadMillis.get();
    }
}
