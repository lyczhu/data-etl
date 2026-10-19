package com.lawyus.dataetl.taskmanagement.orchestrator.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * 新旧链路灰度路由开关（{@code orchestrator-design.md §11.2/§11.4}，方案 §7.1 回滚开关）。
 *
 * <ul>
 *   <li>{@link #newWorker} —— {@code false}（默认）：现网旧链路（{@code TaskScheduler} 直投 task-request、
 *       {@code TaskCompletedListener/TaskFailedListener} 直写 status）行为不变，新编排链路（{@code DispatchService.pump}
 *       与 Progress/Results/Dlt 监听）休眠；{@code true}：新链路接管派工与收敛，旧链路终态消息经
 *       {@code LegacyStatusAdapter} 收敛到 {@code OrchestratorService.transition()}（D-25）。</li>
 *   <li>{@link #shadowCompare} —— 影子运行比对开关（§11.4），仅控制是否额外产出新旧对比日志/报表，不改变任一链路行为。</li>
 * </ul>
 *
 * <p>该设计使 {@code etl.routing.newWorker=false} 能全退旧链路而不退旧状态模型（§11.2）。
 */
@Component
@ConfigurationProperties(prefix = "etl.routing")
public class RoutingProperties {

    /** 新 worker 链路总开关（默认关闭，灰度期与旧链路并存）。 */
    private boolean newWorker = false;
    /** 影子比对（新旧结果差异报表）开关，不影响链路行为。 */
    private boolean shadowCompare = false;

    public boolean isNewWorker() {
        return newWorker;
    }

    public void setNewWorker(boolean newWorker) {
        this.newWorker = newWorker;
    }

    public boolean isShadowCompare() {
        return shadowCompare;
    }

    public void setShadowCompare(boolean shadowCompare) {
        this.shadowCompare = shadowCompare;
    }
}
