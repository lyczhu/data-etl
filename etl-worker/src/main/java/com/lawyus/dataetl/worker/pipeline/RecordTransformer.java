package com.lawyus.dataetl.worker.pipeline;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;

/**
 * 进程内转换 SPI（E→T→L 的 T）。返回新 {@link SourceRecord} 或原样；返回 {@code null} 表示丢弃该行（计入 dropped）。
 * 现默认规则（"数值保留 2 位小数"）由 {@link NumberScaleTransformer} 承接，替换现 {@code DataTransformationService}。
 */
public interface RecordTransformer {

    SourceRecord apply(SourceRecord record);

    /** 空操作转换器（骨架默认，不做任何变换）。 */
    static RecordTransformer identity() {
        return record -> record;
    }
}
