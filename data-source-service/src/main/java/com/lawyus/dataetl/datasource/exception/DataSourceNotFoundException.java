package com.lawyus.dataetl.datasource.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

@ResponseStatus(value = HttpStatus.NOT_FOUND)
public class DataSourceNotFoundException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    public DataSourceNotFoundException(String message) {
        super(message);
    }
    
    public DataSourceNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
}