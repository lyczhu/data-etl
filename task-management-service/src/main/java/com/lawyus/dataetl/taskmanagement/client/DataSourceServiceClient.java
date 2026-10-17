package com.lawyus.dataetl.taskmanagement.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

@FeignClient(name = "data-source-service")
public interface DataSourceServiceClient {
    
    @GetMapping("/api/data-sources/{id}")
    DataSource getDataSourceById(@PathVariable("id") Long dataSourceId);
}