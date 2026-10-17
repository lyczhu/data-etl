package com.lawyus.dataetl.userservice.dto;

import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class RoleResponse {

    private Long id;
    private String name;
    private String code;
    private String description;
    private List<String> permissions;
    private Date createdAt;
}
