package com.lawyus.dataetl.datasource.controller;

import com.lawyus.dataetl.datasource.entity.DataSource;
import com.lawyus.dataetl.datasource.service.DataSourceService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/data-sources")
public class DataSourceController {
    @Autowired
    private DataSourceService dataSourceService;

    @GetMapping
    public List<DataSource> getAllDataSources() {
        return dataSourceService.getAllDataSources();
    }

    @GetMapping("/{id}")
    public ResponseEntity<DataSource> getDataSourceById(@PathVariable(value = "id") Long dataSourceId) {
        DataSource dataSource = dataSourceService.getDataSourceById(dataSourceId)
                .orElseThrow(() -> new RuntimeException("DataSource not found with id " + dataSourceId));
        return ResponseEntity.ok().body(dataSource);
    }

    @PostMapping
    public DataSource createDataSource(@RequestBody DataSource dataSource) {
        return dataSourceService.createDataSource(dataSource);
    }

    @PutMapping("/{id}")
    public ResponseEntity<DataSource> updateDataSource(@PathVariable(value = "id") Long dataSourceId,
                                                   @RequestBody DataSource dataSourceDetails) {
        DataSource updatedDataSource = dataSourceService.updateDataSource(dataSourceId, dataSourceDetails);
        return ResponseEntity.ok(updatedDataSource);
    }

    @DeleteMapping("/{id}")
    public ResponseEntity<Void> deleteDataSource(@PathVariable(value = "id") Long dataSourceId) {
        dataSourceService.deleteDataSource(dataSourceId);
        return ResponseEntity.noContent().build();
    }
}
