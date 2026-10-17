package com.lawyus.dataetl.taskmanagement.entity;

import lombok.Data;

@Data
public class EtlTaskMessage {
    private String taskId;
    private String sourceTableName;

    private DataSource dataSource;

    @Data
    public static class DataSource {

        private Long id;
        private String name;
        private String type;
        private String url;
        private String username;
        private String password;
        private String driverClassName;
    }
}