package com.lawyus.dataetl.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;

/**
 * ETL worker：从 {@code etl.task.dispatch.batch} 领取分片，在<b>进程内</b>跑 SourceReader → Transformer → SinkWriter，
 * 每 chunk 直写 checkpoint，结束发 progress/results（方案 §2.1 / §5.2）。
 *
 * <p>profile=batch（默认，分片批处理）；profile=stream 为常驻流消费者（Phase 3 上线，本骨架只占位）。
 */
@SpringBootApplication
@EnableDiscoveryClient
@ConfigurationPropertiesScan
public class WorkerApplication {

    public static void main(String[] args) {
        SpringApplication.run(WorkerApplication.class, args);
    }
}
