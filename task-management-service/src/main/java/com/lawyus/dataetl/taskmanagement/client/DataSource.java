package com.lawyus.dataetl.taskmanagement.client;

import lombok.Data;

@Data
public class DataSource {

    private Long id;

    private String name;
    private String type;
    private String url;
    private String username;
    private String password;
    private String driverClassName;
    private String description;

}