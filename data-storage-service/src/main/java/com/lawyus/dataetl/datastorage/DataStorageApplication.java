package com.lawyus.dataetl.datastorage;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.openfeign.EnableFeignClients;

@SpringBootApplication
@EnableDiscoveryClient
@EnableFeignClients
public class DataStorageApplication {

    public static void main(String[] args) {
        SpringApplication.run(DataStorageApplication.class, args);
    }
}
