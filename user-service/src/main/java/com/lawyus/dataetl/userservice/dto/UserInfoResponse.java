package com.lawyus.dataetl.userservice.dto;

import java.util.Date;
import java.util.List;

import lombok.Data;

@Data
public class UserInfoResponse {

    private Long id;
    private String username;
    private String nickname;
    private String email;
    private String avatar;
    private List<String> roles;
    private List<String> permissions;
    private Date createdAt;
}
