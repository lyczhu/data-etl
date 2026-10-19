package com.lawyus.dataetl.worker.datasource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据源解析（方案 v2 WP-06 / D10）：派工消息只带 {@code dataSourceId/targetDataSourceId}，
 * worker 在运行时向 data-source-service 换取短期连接信息，避免明文口令落在 Kafka topic/DLT/日志。
 *
 * <p>本地缓存 5 分钟，避免每个分片一次 Feign/HTTP；解析失败直接抛错，由 ShardExecutor 归类为
 * TRANSIENT/PERSISTENT 并重试或落 DLT。
 */
@Component
public class DataSourceResolver {

    private static final Logger log = LoggerFactory.getLogger(DataSourceResolver.class);
    private static final long CACHE_TTL_MS = 5 * 60 * 1000L;

    private final RestTemplate restTemplate;
    private final String baseUrl;
    private final Map<Long, Cached> cache = new ConcurrentHashMap<>();

    public DataSourceResolver(RestTemplateBuilder builder,
                               @Value("${data.source.service.url:http://localhost:8081}") String baseUrl) {
        this.restTemplate = builder
                .setConnectTimeout(Duration.ofSeconds(3))
                .setReadTimeout(Duration.ofSeconds(5))
                .build();
        this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    }

    public ResolvedDataSource resolve(Long dataSourceId) {
        if (dataSourceId == null) {
            throw new IllegalStateException("dispatch message missing dataSourceId");
        }
        long now = System.currentTimeMillis();
        Cached cached = cache.get(dataSourceId);
        if (cached != null && cached.expiresAt > now) {
            return cached.value;
        }
        String url = baseUrl + "/internal/data-sources/" + dataSourceId + "/credentials";
        try {
            ResponseEntity<Map<String, Object>> resp = restTemplate.exchange(
                    url, HttpMethod.GET, null, new ParameterizedTypeReference<>() {
                    });
            Map<String, Object> body = resp.getBody();
            if (body == null) {
                throw new IllegalStateException("empty data source response for id " + dataSourceId);
            }
            ResolvedDataSource resolved = new ResolvedDataSource(
                    dataSourceId,
                    str(body.get("type")),
                    str(body.get("url")),
                    str(body.get("username")),
                    str(body.get("password")),
                    str(body.get("driverClassName")));
            cache.put(dataSourceId, new Cached(resolved, now + CACHE_TTL_MS));
            return resolved;
        } catch (RuntimeException e) {
            log.warn("resolve data source {} failed: {}", dataSourceId, e.getMessage());
            throw e;
        }
    }

    public void evict(Long dataSourceId) {
        if (dataSourceId != null) {
            cache.remove(dataSourceId);
        }
    }

    private static String str(Object v) {
        return v == null ? null : String.valueOf(v);
    }

    private record Cached(ResolvedDataSource value, long expiresAt) {
    }
}
