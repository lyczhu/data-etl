package com.lawyus.dataetl.worker.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;

/**
 * 启用 Kafka 监听。消费/生产工厂与 {@code KafkaTemplate} 由 Spring Boot 依 {@code spring.kafka.*} 自动装配，
 * 此处不覆盖默认工厂（保留 Boot/Sleuth 的自动插桩能力，见 AGENTS.md 关于自定义工厂丢失 trace 的教训）。
 */
@Configuration
@EnableKafka
public class WorkerKafkaConfig {

    // 仅做 @EnableKafka 声明；容器工厂走 Boot 默认自动装配。
}
