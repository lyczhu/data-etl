package com.lawyus.dataetl.datasource.service;

import java.util.List;
import java.util.Optional;

import com.lawyus.dataetl.datasource.security.PasswordCipher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.lawyus.dataetl.datasource.entity.DataSource;
import com.lawyus.dataetl.datasource.exception.DataSourceNotFoundException;
import com.lawyus.dataetl.datasource.repository.DataSourceRepository;

@Service
public class DataSourceService {
    @Autowired
    private DataSourceRepository dataSourceRepository;

    @Autowired
    private PasswordCipher passwordCipher;

    public List<DataSource> getAllDataSources() {
        // WP-06：公开列表不返回任何口令（即使是可解密的历史值）
        return dataSourceRepository.findAll().stream().map(this::maskedCopy).toList();
    }

    /** 公开详情：口令脱敏，仅返回元数据。 */
    public Optional<DataSource> getDataSourceById(Long id) {
        return dataSourceRepository.findById(id).map(this::maskedCopy);
    }

    /**
     * 内部凭据解析（orchestrator V-2 校验 / worker 运行时取数）：返回解密后的连接信息。
     * 只由 {@code /internal/data-sources/**} 暴露，网关不路由该前缀。
     */
    public Optional<DataSource> getCredentialsById(Long id) {
        return dataSourceRepository.findById(id).map(this::decryptedCopy);
    }

    public DataSource createDataSource(DataSource dataSource) {
        dataSource.setPassword(passwordCipher.encrypt(dataSource.getPassword()));
        DataSource saved = dataSourceRepository.save(dataSource);
        // 响应不回传口令（WP-06：REST 脱敏）
        return maskedCopy(saved);
    }

    public DataSource updateDataSource(Long id, DataSource dataSourceDetails) {
        DataSource dataSource = dataSourceRepository.findById(id)
                .orElseThrow(() -> new DataSourceNotFoundException("DataSource not found with id " + id));

        // 使用setter和getter方法
        dataSource.setName(dataSourceDetails.getName());
        dataSource.setType(dataSourceDetails.getType());
        dataSource.setUrl(dataSourceDetails.getUrl());
        dataSource.setUsername(dataSourceDetails.getUsername());
        // 空口令表示“不修改”，避免前端脱敏值覆盖真实口令
        if (dataSourceDetails.getPassword() != null && !dataSourceDetails.getPassword().isBlank()) {
            dataSource.setPassword(passwordCipher.encrypt(dataSourceDetails.getPassword()));
        }
        dataSource.setDriverClassName(dataSourceDetails.getDriverClassName());
        dataSource.setDescription(dataSourceDetails.getDescription());

        return maskedCopy(dataSourceRepository.save(dataSource));
    }

    public void deleteDataSource(Long id) {
        DataSource dataSource = dataSourceRepository.findById(id)
                .orElseThrow(() -> new DataSourceNotFoundException("DataSource not found with id " + id));
        dataSourceRepository.delete(dataSource);
    }

    /** 读取副本：解密口令但不改托管实体，避免 JPA 脏检查把明文写回。 */
    private DataSource decryptedCopy(DataSource source) {
        DataSource copy = new DataSource();
        copy.setId(source.getId());
        copy.setName(source.getName());
        copy.setType(source.getType());
        copy.setUrl(source.getUrl());
        copy.setUsername(source.getUsername());
        copy.setPassword(passwordCipher.decrypt(source.getPassword()));
        copy.setDriverClassName(source.getDriverClassName());
        copy.setDescription(source.getDescription());
        copy.setCreatedAt(source.getCreatedAt());
        copy.setUpdatedAt(source.getUpdatedAt());
        return copy;
    }

    private DataSource maskedCopy(DataSource source) {
        DataSource copy = new DataSource();
        copy.setId(source.getId());
        copy.setName(source.getName());
        copy.setType(source.getType());
        copy.setUrl(source.getUrl());
        copy.setUsername(source.getUsername());
        copy.setPassword(null);
        copy.setDriverClassName(source.getDriverClassName());
        copy.setDescription(source.getDescription());
        copy.setCreatedAt(source.getCreatedAt());
        copy.setUpdatedAt(source.getUpdatedAt());
        return copy;
    }
}
