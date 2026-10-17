package com.lawyus.dataetl.userservice.dto;

import com.lawyus.dataetl.userservice.entity.UserStatus;
import lombok.Data;

import java.util.Date;
import java.util.List;

@Data
public class UserResponse {

    private Long id;
    private String username;
    private String nickname;
    private String email;
    private String phone;
    private UserStatus status;
    private List<String> roles;
    private Date createdAt;
    private Date lastLoginAt;
}
