package com.lawyus.dataetl.datatransformation.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.retry.RetryCallback;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryListener;
import org.springframework.retry.backoff.ExponentialBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

@Configuration
public class RetryConfig {

    private static final Logger log = LoggerFactory.getLogger(RetryConfig.class);

    @Bean("kafkaSendRetryTemplate")
    public RetryTemplate kafkaSendRetryTemplate() {
        RetryTemplate retryTemplate = getRetryTemplate();

        // 可选项：添加重试监听器，记录重试日志
        retryTemplate.registerListener(new RetryListener() {
            @Override
            public <T, E extends Throwable> boolean open(RetryContext context, RetryCallback<T, E> callback) {
                return true;
            }

            @Override
            public <T, E extends Throwable> void close(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
                if (context.getRetryCount() > 0) {
                    log.info("重试完成，共重试 {} 次", context.getRetryCount());
                }
            }

            @Override
            public <T, E extends Throwable> void onError(RetryContext context, RetryCallback<T, E> callback, Throwable throwable) {
                log.warn("第 {} 次重试失败，异常: {}", context.getRetryCount(), throwable.getMessage());
            }
        });
        return retryTemplate;
    }

    private static RetryTemplate getRetryTemplate() {
        RetryTemplate retryTemplate = new RetryTemplate();

        // 重试策略：执行 3 次 （初始 + 重试 2 次）
        SimpleRetryPolicy retryPolicy = new SimpleRetryPolicy();
        retryPolicy.setMaxAttempts(2);
        retryTemplate.setRetryPolicy(retryPolicy);

        // 退避策略：指数退避
        ExponentialBackOffPolicy backOffPolicy = new ExponentialBackOffPolicy();
        backOffPolicy.setInitialInterval(1000);   // 初始间隔 1 秒
        backOffPolicy.setMultiplier(2.0);          // 每次翻倍
        backOffPolicy.setMaxInterval(30000);       // 最大间隔 30 秒
        retryTemplate.setBackOffPolicy(backOffPolicy);
        return retryTemplate;
    }
}