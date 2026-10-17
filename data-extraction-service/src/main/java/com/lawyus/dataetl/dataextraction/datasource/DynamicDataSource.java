package com.lawyus.dataetl.dataextraction.datasource;

import java.io.Serial;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.stereotype.Component;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

/**
 * 动态数据源
 * <p>
 * 负责在运行时根据任务消息中的数据源信息动态创建、缓存和管理 HikariCP 连接池。
 * 按 dataSourceId 缓存 DataSource 实例，避免重复创建连接池。
 * </p>
 * <p>
 * <b>使用注意：</b>
 * <ul>
 *   <li>缓存命中后，url/username/password/driverClassName 入参将被忽略（以首次缓存配置为准）。</li>
 *   <li>若数据源配置（特别是密码）发生变更，请先调用 {@link #removeDataSource(String)}。</li>
 *   <li>缓存容量有上限，达到上限后按 LRU 策略淘汰最久未使用的数据源。</li>
 * </ul>
 * </p>
 */
@RefreshScope
@Component
public class DynamicDataSource {

    private static final Logger log = LoggerFactory.getLogger(DynamicDataSource.class);

    /**
     * LRU 缓存：key=dataSourceId, value=HikariDataSource。
     * 使用 {@link Collections#synchronizedMap} 保证线程安全，
     * 容量上限受 {@code maxCacheSize} 控制。
     */
    private Map<String, HikariDataSource> dataSourceCache;

    @Value("${datasource.dynamic.max-pool-size:10}")
    private int maxPoolSize;

    @Value("${datasource.dynamic.min-idle:2}")
    private int minIdle;

    @Value("${datasource.dynamic.connection-timeout:30000}")
    private long connectionTimeoutMs;

    @Value("${datasource.dynamic.idle-timeout:600000}")
    private long idleTimeoutMs;

    @Value("${datasource.dynamic.max-lifetime:1800000}")
    private long maxLifetimeMs;

    @Value("${datasource.dynamic.keepalive-time:300000}")
    private long keepaliveTimeMs;

    @Value("${datasource.dynamic.leak-detection-threshold:60000}")
    private long leakDetectionThresholdMs;

    @Value("${datasource.dynamic.max-cache-size:50}")
    private int maxCacheSize;

    private final Object lock = new Object();

    @PostConstruct
    void init() {
        int capacity = Math.max(1, maxCacheSize);
        // accessOrder=true 实现访问顺序 LRU，removeEldestEntry 在插入后自动淘汰最久未访问条目
        this.dataSourceCache = Collections.synchronizedMap(
                new LinkedHashMap<>(16, 0.75f, true) {
                    @Serial
                    private static final long serialVersionUID = 1L;

                    @Override
                    protected boolean removeEldestEntry(Map.Entry<String, HikariDataSource> eldest) {
                        if (size() > capacity) {
                            closeQuietly(eldest.getKey(), eldest.getValue());
                            return true;
                        }
                        return false;
                    }
                });
    }

    /**
     * 获取或创建数据源
     * <p>
     * <b>注意：</b>当 dataSourceId 已在缓存中时，url/username/password/driverClassName
     * 等参数将被忽略，以首次缓存的配置为准。若需刷新配置，请先调用
     * {@link #removeDataSource(String)}。
     * </p>
     *
     * @param dataSourceId   数据源唯一标识
     * @param url            数据库连接 URL（仅在缓存未命中时使用）
     * @param username       数据库用户名（仅在缓存未命中时使用）
     * @param password       数据库密码（仅在缓存未命中时使用）
     * @param driverClassName JDBC 驱动类名（仅在缓存未命中时使用）
     * @return 数据源实例
     */
    public DataSource getOrCreateDataSource(Long dataSourceId, String url, String username,
                                            String password, String driverClassName) {
        String key = String.valueOf(dataSourceId);
        // synchronizedMap 的 get/put 各自原子，但 get-then-put 需要在锁内串行化以避免 LRU 重复创建
        synchronized (lock) {
            HikariDataSource existing = dataSourceCache.get(key);
            if (existing != null) {
                return existing;
            }
            log.info("Creating new DataSource for dataSourceId: {}, sanitizedUrl: {}",
                    key, sanitizeUrl(url));
            HikariDataSource ds = createDataSource(dataSourceId, url, username, password, driverClassName);
            dataSourceCache.put(key, ds);
            return ds;
        }
    }

    /**
     * 获取数据源连接
     *
     * @param dataSourceId   数据源唯一标识
     * @param url            数据库连接 URL（仅在缓存未命中时使用）
     * @param username       数据库用户名（仅在缓存未命中时使用）
     * @param password       数据库密码（仅在缓存未命中时使用）
     * @param driverClassName JDBC 驱动类名（仅在缓存未命中时使用）
     * @return 数据库连接
     * @throws SQLException 连接异常
     */
    public Connection getConnection(Long dataSourceId, String url, String username,
                                    String password, String driverClassName) throws SQLException {
        DataSource dataSource = getOrCreateDataSource(dataSourceId, url, username, password, driverClassName);
        return dataSource.getConnection();
    }

    /**
     * 移除并关闭指定数据源
     *
     * @param dataSourceId 数据源唯一标识
     */
    public void removeDataSource(String dataSourceId) {
        if (dataSourceId == null || dataSourceCache == null) {
            return;
        }
        synchronized (lock) {
            HikariDataSource removed = dataSourceCache.remove(dataSourceId);
            if (removed != null) {
                closeQuietly(dataSourceId, removed);
            }
        }
    }

    /**
     * 关闭所有数据源。在 Spring 容器销毁时自动调用。
     */
    @PreDestroy
    public void closeAll() {
        if (dataSourceCache == null) {
            return;
        }
        synchronized (lock) {
            dataSourceCache.forEach(this::closeQuietly);
            dataSourceCache.clear();
        }
    }

    /**
     * 获取当前缓存的数据源数量
     */
    public int getDataSourceCount() {
        return dataSourceCache == null ? 0 : dataSourceCache.size();
    }

    private HikariDataSource createDataSource(Long dataSourceId, String url, String username,
                                              String password, String driverClassName) {
        if (url == null || url.isEmpty()) {
            throw new IllegalArgumentException("jdbc url must not be empty for dataSourceId=" + dataSourceId);
        }

        // 驱动类预校验：避免错误延迟到首次取连接时才暴露
        if (driverClassName != null && !driverClassName.isEmpty()) {
            try {
                Class.forName(driverClassName);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException(
                        "Driver class not found: " + driverClassName + " for dataSourceId=" + dataSourceId, e);
            }
        }

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        config.setUsername(username);
        config.setPassword(password);

        if (driverClassName != null && !driverClassName.isEmpty()) {
            config.setDriverClassName(driverClassName);
        }

        // 连接池配置
        config.setMaximumPoolSize(maxPoolSize);
        config.setMinimumIdle(minIdle);
        config.setConnectionTimeout(connectionTimeoutMs);
        config.setIdleTimeout(idleTimeoutMs);
        config.setMaxLifetime(maxLifetimeMs);
        config.setKeepaliveTime(keepaliveTimeMs);
        config.setLeakDetectionThreshold(leakDetectionThresholdMs);

        // 4.x 默认通过 JDBC4 Connection.isValid() 校验，不再硬编码 connectionTestQuery（避免 Oracle 等兼容问题）

        // 连接池命名，便于监控（以 dataSourceId 命名，避免 hashCode 冲突）
        config.setPoolName("DS-" + dataSourceId);

        // 启用 JMX MBean 监控
        config.setRegisterMbeans(true);

        // 性能配置
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");

        return new HikariDataSource(config);
    }

    /**
     * 静默关闭数据源，异常仅记录不抛出。
     */
    private void closeQuietly(String key, HikariDataSource ds) {
        if (ds == null) {
            return;
        }
        try {
            if (!ds.isClosed()) {
                ds.close();
                log.info("Closed DataSource for dataSourceId: {}", key);
            }
        } catch (Exception ex) {
            log.warn("Failed to close DataSource for dataSourceId: {}", key, ex);
        }
    }

    /**
     * 对 JDBC URL 进行脱敏，移除 userinfo 部分，避免日志中泄露凭据。
     */
    private static String sanitizeUrl(String url) {
        if (url == null) {
            return null;
        }
        try {
            URI uri = new URI(url);
            if (uri.getUserInfo() != null) {
                return url.replace(uri.getUserInfo() + "@", "***@");
            }
            return url;
        } catch (Exception ex) {
            // 非标准 URI 格式时仅保留 jdbc 协议前缀
            int idx = url.indexOf("://");
            return idx > 0 ? url.substring(0, idx + 3) + "***" : "***";
        }
    }
}
