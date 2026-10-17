-- 用户管理服务初始化数据

-- 1. 插入默认角色
INSERT INTO sys_role (name, code, description, status, sort_order) VALUES
('管理员', 'ADMIN', '系统管理员，拥有所有权限', 'ACTIVE', 1),
('操作员', 'OPERATOR', '数据操作人员，可管理数据源和任务', 'ACTIVE', 2),
('查看者', 'VIEWER', '只读权限用户，仅可查看数据', 'ACTIVE', 3);

-- 2. 插入默认权限
INSERT INTO sys_permission (name, code, type, parent_id, path, icon, sort_order, status) VALUES
-- 菜单权限
('仪表盘', 'dashboard', 'MENU', 0, '/dashboard', 'Odometer', 1, 'ACTIVE'),
('数据源管理', 'datasource', 'MENU', 0, '/data-source', 'Coin', 2, 'ACTIVE'),
('任务管理', 'task', 'MENU', 0, '/task', 'List', 3, 'ACTIVE'),
('网关管理', 'gateway', 'MENU', 0, '/gateway', 'Guide', 4, 'ACTIVE'),
('系统管理', 'system', 'MENU', 0, '/system', 'Setting', 5, 'ACTIVE'),
-- 按钮权限 - 数据源
('数据源查看', 'datasource:read', 'BUTTON', 2, NULL, NULL, 1, 'ACTIVE'),
('数据源新增', 'datasource:write', 'BUTTON', 2, NULL, NULL, 2, 'ACTIVE'),
-- 按钮权限 - 任务
('任务查看', 'task:read', 'BUTTON', 3, NULL, NULL, 1, 'ACTIVE'),
('任务新增', 'task:write', 'BUTTON', 3, NULL, NULL, 2, 'ACTIVE'),
-- 按钮权限 - 网关
('网关查看', 'gateway:read', 'BUTTON', 4, NULL, NULL, 1, 'ACTIVE'),
('网关编辑', 'gateway:write', 'BUTTON', 4, NULL, NULL, 2, 'ACTIVE'),
-- 按钮权限 - 系统
('系统查看', 'system:read', 'BUTTON', 5, NULL, NULL, 1, 'ACTIVE'),
('系统编辑', 'system:write', 'BUTTON', 5, NULL, NULL, 2, 'ACTIVE');

-- 3. 插入默认管理员用户 (密码: admin123, 使用BCrypt加密)
INSERT INTO sys_user (username, password, nickname, email, phone, status) VALUES
('admin', '$2a$10$N.zmdr9k7uOCQb376NoUnuTJ8iAt6Z5EHsM8lE9lBOsl7iAt6Z5EH', '管理员', 'admin@example.com', '13800138000', 'ACTIVE');

-- 4. 分配管理员角色
INSERT INTO sys_user_role (user_id, role_id) VALUES (1, 1);

-- 5. 分配角色权限
-- 管理员角色拥有所有权限
INSERT INTO sys_role_permission (role_id, permission_id)
SELECT 1, id FROM sys_permission;

-- 操作员角色权限
INSERT INTO sys_role_permission (role_id, permission_id) VALUES
(2, 1), (2, 2), (2, 3), (2, 4),
(2, 6), (2, 7), (2, 8), (2, 9), (2, 10);

-- 查看者角色权限
INSERT INTO sys_role_permission (role_id, permission_id) VALUES
(3, 1), (3, 2), (3, 3), (3, 4),
(3, 6), (3, 8), (3, 10);

-- 6. 插入演示用户
-- 操作员用户 (密码: operator123)
INSERT INTO sys_user (username, password, nickname, email, phone, status) VALUES
('operator', '$2a$10$N.zmdr9k7uOCQb376NoUnuTJ8iAt6Z5EHsM8lE9lBOsl7iAt6Z5EH', '操作员', 'operator@example.com', '13800138001', 'ACTIVE');
INSERT INTO sys_user_role (user_id, role_id) VALUES (2, 2);

-- 查看者用户 (密码: viewer123)
INSERT INTO sys_user (username, password, nickname, email, phone, status) VALUES
('viewer', '$2a$10$N.zmdr9k7uOCQb376NoUnuTJ8iAt6Z5EHsM8lE9lBOsl7iAt6Z5EH', '查看者', 'viewer@example.com', '13800138002', 'INACTIVE');
INSERT INTO sys_user_role (user_id, role_id) VALUES (3, 3);
