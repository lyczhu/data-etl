package com.lawyus.dataetl.userservice.exception;

public class RoleNotFoundException extends RuntimeException {

    public RoleNotFoundException(String message) {
        super(message);
    }

    public RoleNotFoundException(Long id) {
        super("Role not found with id: " + id);
    }
}
