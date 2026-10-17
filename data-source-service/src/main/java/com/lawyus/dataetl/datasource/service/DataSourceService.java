package com.lawyus.dataetl.datasource.service;

import java.util.List;
import java.util.Optional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lawyus.dataetl.datasource.entity.DataSource;
import com.lawyus.dataetl.datasource.exception.DataSourceNotFoundException;
import com.lawyus.dataetl.datasource.repository.DataSourceRepository;

@Service
public class DataSourceService {
    @Autowired
    private DataSourceRepository dataSourceRepository;

    public List<DataSource> getAllDataSources() {
        return dataSourceRepository.findAll();
    }

    public Optional<DataSource> getDataSourceById(Long id) {
        return dataSourceRepository.findById(id);
    }

    public DataSource createDataSource(DataSource dataSource) {
        return dataSourceRepository.save(dataSource);
    }

    public DataSource updateDataSource(Long id, DataSource dataSourceDetails) {
        DataSource dataSource = dataSourceRepository.findById(id)
                .orElseThrow(() -> new DataSourceNotFoundException("DataSource not found with id " + id));

        // 使用setter和getter方法
        dataSource.setName(dataSourceDetails.getName());
        dataSource.setType(dataSourceDetails.getType());
        dataSource.setUrl(dataSourceDetails.getUrl());
        dataSource.setUsername(dataSourceDetails.getUsername());
        dataSource.setPassword(dataSourceDetails.getPassword());
        dataSource.setDriverClassName(dataSourceDetails.getDriverClassName());
        dataSource.setDescription(dataSourceDetails.getDescription());

        return dataSourceRepository.save(dataSource);
    }

    public void deleteDataSource(Long id) {
        DataSource dataSource = dataSourceRepository.findById(id)
                .orElseThrow(() -> new DataSourceNotFoundException("DataSource not found with id " + id));
        dataSourceRepository.delete(dataSource);
    }
}
