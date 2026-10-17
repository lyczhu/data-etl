package com.lawyus.dataetl.datastorage.client;

import lombok.Data;

import java.util.Date;

@Data
public class EtlTask {
    private Long id;

    private String taskId;
    private Long dataSourceId;
    private String sourceTableName;
    private String targetTableName;
    private String status;
    // 修复：字段名/类型与服务端 entity.EtlTask 对齐（服务端为 totalRecords/completedRecords:Long，无 shards 字段）
    private Long totalRecords;
    private Long completedRecords;
    private String description;

    private Date createdAt;

    private Date updatedAt;

    private Date startTime;

    private Date endTime;

    private String errorMessage;
}
