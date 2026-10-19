package com.lawyus.dataetl.datasource.controller;

import com.lawyus.dataetl.datasource.entity.DataSource;
import com.lawyus.dataetl.datasource.service.DataSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * 内部凭据端点（WP-06）：只给 orchestrator 的 V-2 校验与 worker 运行时解析使用。
 *
 * <p>刻意使用 {@code /internal/**} 而不是 {@code /api/data-sources/**}：网关只路由后者，
 * 因此该端点不会通过 gateway 暴露给前端。Phase 1B 再替换为短期令牌/Vault。
 */
@RestController
@RequestMapping("/internal/data-sources")
public class InternalDataSourceController {

    @Autowired
    private DataSourceService dataSourceService;

    @GetMapping("/{id}/credentials")
    public ResponseEntity<DataSource> credentials(@PathVariable("id") Long dataSourceId) {
        DataSource dataSource = dataSourceService.getCredentialsById(dataSourceId)
                .orElseThrow(() -> new RuntimeException("DataSource not found with id " + dataSourceId));
        return ResponseEntity.ok(dataSource);
    }
}
