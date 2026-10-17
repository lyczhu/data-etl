package com.lawyus.dataetl.userservice.controller;

import com.lawyus.dataetl.userservice.dto.*;
import com.lawyus.dataetl.userservice.security.JwtTokenProvider;
import com.lawyus.dataetl.userservice.service.UserService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;

@Tag(name = "认证管理", description = "用户认证相关接口")
@RestController
@RequestMapping("/api/auth")
@RequiredArgsConstructor
public class AuthController {

    private final UserService userService;
    private final JwtTokenProvider tokenProvider;

    @Operation(summary = "用户登录")
    @PostMapping("/login")
    public ResponseEntity<LoginResponse> login(@Valid @RequestBody LoginRequest request,
                                               HttpServletRequest httpRequest) {
        LoginResponse response = userService.login(request, httpRequest);
        return ResponseEntity.ok(response);
    }

    @Operation(summary = "用户登出")
    @PostMapping("/logout")
    public ResponseEntity<Void> logout() {
        userService.logout();
        return ResponseEntity.ok().build();
    }

    @Operation(summary = "获取当前用户信息")
    @GetMapping("/user-info")
    public ResponseEntity<UserInfoResponse> getUserInfo(@AuthenticationPrincipal UserDetails userDetails) {
        UserInfoResponse response = userService.getUserInfo(userDetails.getUsername());
        return ResponseEntity.ok(response);
    }

    @Operation(summary = "刷新Token")
    @PostMapping("/refresh")
    public ResponseEntity<LoginResponse> refreshToken(@AuthenticationPrincipal UserDetails userDetails) {
        LoginResponse response = userService.refreshToken(userDetails.getUsername());
        return ResponseEntity.ok(response);
    }
}
