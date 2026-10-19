package com.lawyus.dataetl.worker.pipeline;

import com.lawyus.dataetl.worker.pipeline.transform.DateNormalizeTransformer;
import com.lawyus.dataetl.worker.pipeline.transform.FieldPruneTransformer;
import com.lawyus.dataetl.worker.pipeline.transform.GeoJsonToWktTransformer;
import com.lawyus.dataetl.worker.pipeline.transform.StringTrimTransformer;
import com.lawyus.dataetl.worker.pipeline.transform.TypeCoercionTransformer;

import java.util.ArrayList;
import java.util.List;

/**
 * 按 {@link DataProfile} 组装转换规则链（T 阶段的可编排入口）。
 *
 * <p>把"转换器"按接入数据形态串成合理顺序：先整形（字段裁剪/修剪空白），再归一（日期），
 * 后还原类型（{@link TypeCoercionTransformer} 必须在日期归一之后，避免 epoch 串被先转数字），
 * 最后统一数值精度。{@code keepColumns} 非空时前置 {@link FieldPruneTransformer}。
 *
 * <p>Phase 2 起规则链应由任务配置（每任务的转换 DSL）驱动；本工厂先用数据形态档位提供可运行的模拟默认。
 */
public final class TransformerFactory {

    private TransformerFactory() {
    }

    public static RecordTransformer forProfile(DataProfile profile, List<String> keepColumns, int numberScale) {
        List<RecordTransformer> steps = new ArrayList<>();
        if (keepColumns != null && !keepColumns.isEmpty()) {
            steps.add(new FieldPruneTransformer(keepColumns, true));
        }
        switch (profile) {
            case CSV:
            case MONGO:
                steps.add(new StringTrimTransformer());
                steps.add(new DateNormalizeTransformer());
                steps.add(new TypeCoercionTransformer());
                steps.add(new NumberScaleTransformer(numberScale));
                break;
            case JSON:
                steps.add(new DateNormalizeTransformer());
                steps.add(new TypeCoercionTransformer());
                steps.add(new NumberScaleTransformer(numberScale));
                break;
            case GEOJSON:
                steps.add(new StringTrimTransformer());
                steps.add(new DateNormalizeTransformer());
                steps.add(new TypeCoercionTransformer());
                steps.add(new GeoJsonToWktTransformer());
                steps.add(new NumberScaleTransformer(numberScale));
                break;
            case JDBC:
            default:
                steps.add(new DateNormalizeTransformer());
                steps.add(new NumberScaleTransformer(numberScale));
                break;
        }
        return CompositeTransformer.of(steps.toArray(new RecordTransformer[0]));
    }
}
