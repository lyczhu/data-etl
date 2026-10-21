package com.lawyus.dataetl.taskmanagement.orchestrator.target;

/** 目标表名已被其它 task 占用（API 映射 409）。 */
public class TargetTableConflictException extends RuntimeException {

    public TargetTableConflictException(String message) {
        super(message);
    }
}
