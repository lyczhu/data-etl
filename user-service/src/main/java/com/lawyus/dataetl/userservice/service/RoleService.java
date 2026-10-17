package com.lawyus.dataetl.userservice.service;

import com.lawyus.dataetl.userservice.dto.RoleRequest;
import com.lawyus.dataetl.userservice.dto.RoleResponse;
import com.lawyus.dataetl.userservice.entity.Permission;
import com.lawyus.dataetl.userservice.entity.Role;
import com.lawyus.dataetl.userservice.exception.BusinessException;
import com.lawyus.dataetl.userservice.exception.RoleNotFoundException;
import com.lawyus.dataetl.userservice.repository.PermissionRepository;
import com.lawyus.dataetl.userservice.repository.RoleRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class RoleService {

    private final RoleRepository roleRepository;
    private final PermissionRepository permissionRepository;

    public List<RoleResponse> getAllRoles() {
        return roleRepository.findAll().stream()
                .map(this::convertToRoleResponse)
                .collect(Collectors.toList());
    }

    public RoleResponse getRoleById(Long id) {
        Role role = roleRepository.findById(id)
                .orElseThrow(() -> new RoleNotFoundException(id));
        return convertToRoleResponse(role);
    }

    @Transactional
    public RoleResponse createRole(RoleRequest request) {
        if (roleRepository.existsByCode(request.getCode())) {
            throw new BusinessException(400, "角色编码已存在");
        }

        Role role = new Role();
        role.setName(request.getName());
        role.setCode(request.getCode());
        role.setDescription(request.getDescription());

        if (request.getPermissions() != null && !request.getPermissions().isEmpty()) {
            List<Permission> permissions = permissionRepository.findByCodeIn(request.getPermissions());
            role.setPermissions(permissions);
        }

        role = roleRepository.save(role);
        return convertToRoleResponse(role);
    }

    @Transactional
    public RoleResponse updateRole(Long id, RoleRequest request) {
        Role role = roleRepository.findById(id)
                .orElseThrow(() -> new RoleNotFoundException(id));

        if (request.getName() != null) {
            role.setName(request.getName());
        }
        if (request.getCode() != null && !request.getCode().equals(role.getCode())) {
            if (roleRepository.existsByCode(request.getCode())) {
                throw new BusinessException(400, "角色编码已存在");
            }
            role.setCode(request.getCode());
        }
        if (request.getDescription() != null) {
            role.setDescription(request.getDescription());
        }
        if (request.getPermissions() != null) {
            List<Permission> permissions = permissionRepository.findByCodeIn(request.getPermissions());
            role.setPermissions(permissions);
        }

        role = roleRepository.save(role);
        return convertToRoleResponse(role);
    }

    @Transactional
    public void deleteRole(Long id) {
        Role role = roleRepository.findById(id)
                .orElseThrow(() -> new RoleNotFoundException(id));
        roleRepository.delete(role);
    }

    private RoleResponse convertToRoleResponse(Role role) {
        RoleResponse response = new RoleResponse();
        response.setId(role.getId());
        response.setName(role.getName());
        response.setCode(role.getCode());
        response.setDescription(role.getDescription());
        response.setCreatedAt(role.getCreatedAt());
        response.setPermissions(role.getPermissions().stream()
                .map(Permission::getCode)
                .collect(Collectors.toList()));
        return response;
    }
}
