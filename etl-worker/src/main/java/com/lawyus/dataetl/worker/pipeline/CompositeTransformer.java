package com.lawyus.dataetl.worker.pipeline;

import com.lawyus.dataetl.connector.spi.model.SourceRecord;

import java.util.Arrays;
import java.util.List;

/**
 * 转换器链（T 阶段可编排）。按顺序依次 {@code apply}，任一环节返回 {@code null} 即整行丢弃（计入 dropped）。
 * 空链等价恒等。用于把"按接入类型挑选的若干示例转换器"串成一条流水线。
 */
public class CompositeTransformer implements RecordTransformer {

    private final List<RecordTransformer> steps;

    public CompositeTransformer(List<RecordTransformer> steps) {
        this.steps = steps == null ? List.of() : List.copyOf(steps);
    }

    public static CompositeTransformer of(RecordTransformer... steps) {
        return new CompositeTransformer(Arrays.asList(steps));
    }

    @Override
    public SourceRecord apply(SourceRecord record) {
        SourceRecord cur = record;
        for (RecordTransformer step : steps) {
            if (cur == null) {
                return null;
            }
            cur = step.apply(cur);
        }
        return cur;
    }
}
