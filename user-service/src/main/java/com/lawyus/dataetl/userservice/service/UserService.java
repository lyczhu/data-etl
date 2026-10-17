package com.lawyus.dataetl.userservice.service;

import java.util.Date;
import java.util.List;
import javax.servlet.http.HttpServletRequest;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.lawyus.dataetl.userservice.dto.LoginRequest;
import com.lawyus.dataetl.userservice.dto.LoginResponse;
import com.lawyus.dataetl.userservice.dto.UserInfoResponse;
import com.lawyus.dataetl.userservice.dto.UserRequest;
import com.lawyus.dataetl.userservice.dto.UserResponse;
import com.lawyus.dataetl.userservice.entity.Permission;
import com.lawyus.dataetl.userservice.entity.Role;
import com.lawyus.dataetl.userservice.entity.User;
import com.lawyus.dataetl.userservice.entity.UserStatus;
import com.lawyus.dataetl.userservice.exception.BusinessException;
import com.lawyus.dataetl.userservice.exception.UserNotFoundException;
import com.lawyus.dataetl.userservice.repository.RoleRepository;
import com.lawyus.dataetl.userservice.repository.UserRepository;
import com.lawyus.dataetl.userservice.security.JwtTokenProvider;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final RoleRepository roleRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtTokenProvider tokenProvider;
    private final AuthenticationManager authenticationManager;

    @Transactional
    public LoginResponse login(LoginRequest request, HttpServletRequest httpRequest) {
        Authentication authentication = authenticationManager.authenticate(
                new UsernamePasswordAuthenticationToken(request.getUsername(), request.getPassword())
        );

        SecurityContextHolder.getContext().setAuthentication(authentication);

        User user = userRepository.findByUsername(request.getUsername())
                .orElseThrow(() -> new BusinessException(401, "用户名或密码错误"));

        if (user.getStatus() != UserStatus.ACTIVE) {
            throw new BusinessException(401, "账户已被禁用或锁定");
        }

        List<String> roles = user.getRoles().stream()
                .map(Role::getCode).toList();

        String token = tokenProvider.generateToken(user.getUsername(), roles);

        user.setLastLoginAt(new Date());
        user.setLastLoginIp(getClientIp(httpRequest));
        userRepository.save(user);

        return new LoginResponse(token);
    }

    @Transactional
    public void logout() {
        SecurityContextHolder.clearContext();
    }

    public UserInfoResponse getUserInfo(String username) {
        User user = userRepository.findByUsername(username)
                .orElseThrow(() -> new UserNotFoundException("用户不存在"));

        List<String> roles = user.getRoles().stream()
                .map(Role::getCode).toList();

        List<String> permissions = user.getRoles().stream()
                .flatMap(role -> role.getPermissions().stream())
                .map(Permission::getCode)
                .distinct().toList();

        UserInfoResponse response = new UserInfoResponse();
        response.setId(user.getId());
        response.setUsername(user.getUsername());
        response.setNickname(user.getNickname());
        response.setEmail(user.getEmail());
        response.setAvatar(user.getAvatar());
        response.setRoles(roles);
        response.setPermissions(permissions);
        response.setCreatedAt(user.getCreatedAt());

        return response;
    }

    public LoginResponse refreshToken(String username) {
        User user = userRepository.findByUsername(username)
                .orElseThrow(() -> new UserNotFoundException("用户不存在"));

        List<String> roles = user.getRoles().stream()
                .map(Role::getCode).toList();

        String token = tokenProvider.generateToken(username, roles);
        return new LoginResponse(token);
    }

    public List<UserResponse> getAllUsers() {
        return userRepository.findAll().stream()
                .map(this::convertToUserResponse).toList();
    }

    public Page<UserResponse> getUsers(Pageable pageable) {
        return userRepository.findAll(pageable)
                .map(this::convertToUserResponse);
    }

    public UserResponse getUserById(Long id) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new UserNotFoundException(id));
        return convertToUserResponse(user);
    }

    @Transactional
    public UserResponse createUser(UserRequest request) {
        if (userRepository.existsByUsername(request.getUsername())) {
            throw new BusinessException(400, "用户名已存在");
        }

        User user = new User();
        user.setUsername(request.getUsername());
        user.setPassword(passwordEncoder.encode(request.getPassword()));
        user.setNickname(request.getNickname());
        user.setEmail(request.getEmail());
        user.setPhone(request.getPhone());
        user.setStatus(request.getStatus() != null ? request.getStatus() : UserStatus.ACTIVE);

        if (request.getRoles() != null && !request.getRoles().isEmpty()) {
            List<Role> roles = request.getRoles().stream()
                    .map(code -> roleRepository.findByCode(code)
                            .orElseThrow(() -> new BusinessException(400, "角色不存在: " + code)))
                    .toList();
            user.setRoles(roles);
        }

        user = userRepository.save(user);
        return convertToUserResponse(user);
    }

    @Transactional
    public UserResponse updateUser(Long id, UserRequest request) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new UserNotFoundException(id));

        if (request.getNickname() != null) {
            user.setNickname(request.getNickname());
        }
        if (request.getEmail() != null) {
            user.setEmail(request.getEmail());
        }
        if (request.getPhone() != null) {
            user.setPhone(request.getPhone());
        }
        if (request.getStatus() != null) {
            user.setStatus(request.getStatus());
        }
        if (request.getPassword() != null && !request.getPassword().isEmpty()) {
            user.setPassword(passwordEncoder.encode(request.getPassword()));
        }
        if (request.getRoles() != null) {
            List<Role> roles = request.getRoles().stream()
                    .map(code -> roleRepository.findByCode(code)
                            .orElseThrow(() -> new BusinessException(400, "角色不存在: " + code)))
                    .toList();
            user.setRoles(roles);
        }

        user = userRepository.save(user);
        return convertToUserResponse(user);
    }

    @Transactional
    public void deleteUser(Long id) {
        User user = userRepository.findById(id)
                .orElseThrow(() -> new UserNotFoundException(id));
        userRepository.delete(user);
    }

    private UserResponse convertToUserResponse(User user) {
        UserResponse response = new UserResponse();
        response.setId(user.getId());
        response.setUsername(user.getUsername());
        response.setNickname(user.getNickname());
        response.setEmail(user.getEmail());
        response.setPhone(user.getPhone());
        response.setStatus(user.getStatus());
        response.setCreatedAt(user.getCreatedAt());
        response.setLastLoginAt(user.getLastLoginAt());
        response.setRoles(user.getRoles().stream()
                .map(Role::getCode)
                .toList());
        return response;
    }

    private String getClientIp(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("X-Real-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        if (ip != null && ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }
        return ip;
    }
}
