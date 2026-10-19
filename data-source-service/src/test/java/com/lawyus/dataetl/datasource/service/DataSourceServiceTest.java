package com.lawyus.dataetl.datasource.service;

import com.lawyus.dataetl.datasource.entity.DataSource;
import com.lawyus.dataetl.datasource.repository.DataSourceRepository;
import com.lawyus.dataetl.datasource.security.PasswordCipher;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class DataSourceServiceTest {

    private DataSourceRepository repository;
    private DataSourceService service;
    private PasswordCipher cipher;

    @BeforeEach
    void setUp() {
        repository = mock(DataSourceRepository.class);
        cipher = new PasswordCipher();
        ReflectionTestUtils.setField(cipher, "secretKey", "unit-test-secret");
        ReflectionTestUtils.invokeMethod(cipher, "init");

        service = new DataSourceService();
        ReflectionTestUtils.setField(service, "dataSourceRepository", repository);
        ReflectionTestUtils.setField(service, "passwordCipher", cipher);
    }

    private DataSource stored() {
        DataSource ds = new DataSource();
        ds.setId(1L);
        ds.setName("pg");
        ds.setType("postgresql");
        ds.setUrl("jdbc:postgresql://localhost/etl");
        ds.setUsername("postgres");
        ds.setPassword(cipher.encrypt("real-secret"));
        ds.setDriverClassName("org.postgresql.Driver");
        return ds;
    }

    @Test
    @DisplayName("公开列表/详情不返回口令；内部凭据端点返回解密值")
    void publicReadsAreMaskedAndCredentialsAreDecrypted() {
        DataSource ds = stored();
        when(repository.findAll()).thenReturn(List.of(ds));
        when(repository.findById(1L)).thenReturn(Optional.of(ds));

        assertThat(service.getAllDataSources()).singleElement()
                .satisfies(d -> assertThat(d.getPassword()).isNull());
        assertThat(service.getDataSourceById(1L).orElseThrow().getPassword()).isNull();
        assertThat(service.getCredentialsById(1L).orElseThrow().getPassword()).isEqualTo("real-secret");
    }

    @Test
    @DisplayName("创建时落库加密、响应脱敏")
    void createEncryptsAtRestAndMasksResponse() {
        AtomicReference<String> savedPassword = new AtomicReference<>();
        when(repository.save(any(DataSource.class))).thenAnswer(invocation -> {
            DataSource toSave = invocation.getArgument(0);
            savedPassword.set(toSave.getPassword());
            toSave.setId(7L);
            return toSave;
        });

        DataSource request = new DataSource();
        request.setName("new");
        request.setPassword("plain-secret");
        DataSource response = service.createDataSource(request);

        assertThat(savedPassword.get()).startsWith("ENC(v1:").doesNotContain("plain-secret");
        assertThat(response.getPassword()).isNull();
        assertThat(response.getId()).isEqualTo(7L);
    }

    @Test
    @DisplayName("更新时空口令表示不修改，避免脱敏值覆盖真实口令")
    void updateWithBlankPasswordKeepsStoredSecret() {
        DataSource ds = stored();
        String storedCipher = ds.getPassword();
        when(repository.findById(1L)).thenReturn(Optional.of(ds));
        when(repository.save(any(DataSource.class))).thenAnswer(inv -> inv.getArgument(0));

        DataSource update = new DataSource();
        update.setName("renamed");
        update.setType("postgresql");
        update.setUrl("jdbc:postgresql://localhost/etl");
        update.setUsername("postgres");
        update.setPassword("");
        service.updateDataSource(1L, update);

        assertThat(ds.getPassword()).isEqualTo(storedCipher);   // 密文未被空口令覆盖
        assertThat(cipher.decrypt(ds.getPassword())).isEqualTo("real-secret");
    }
}
