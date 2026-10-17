package com.lawyus.dataetl.userservice.entity;

import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.*;
import java.util.Date;

@Data
@Entity
@Table(name = "sys_permission")
public class Permission {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false, length = 50)
    private String name;

    @Column(nullable = false, unique = true, length = 100)
    private String code;

    @Column(nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private PermissionType type = PermissionType.MENU;

    @Column(name = "parent_id")
    private Long parentId = 0L;

    @Column(length = 255)
    private String path;

    @Column(length = 100)
    private String icon;

    @Column(name = "sort_order")
    private Integer sortOrder = 0;

    @Column(nullable = false, length = 20)
    @Enumerated(EnumType.STRING)
    private PermissionStatus status = PermissionStatus.ACTIVE;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private Date createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private Date updatedAt;
}
