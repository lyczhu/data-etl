import urllib.request
import urllib.parse
import json
import time

# 1. 创建数据源
def create_datasource():
    print("\n1. 创建数据源...")
    # 数据源配置
    datasource_config = {
        "name": "shenzhen_postgres",
        "type": "postgresql",
        "url": "jdbc:postgresql://localhost:5432/shenzhen_db?currentSchema=public",
        "username": "postgres",
        "password": "shenzhen2026",
        "driverClassName": "org.postgresql.Driver",
        "description": "深圳数据库"
    }

    try:
        # 准备请求数据
        url = "http://localhost:8081/api/data-sources"
        data = json.dumps(datasource_config).encode('utf-8')
        headers = {
            'Content-Type': 'application/json',
            'Content-Length': str(len(data))
        }
        
        # 创建请求
        req = urllib.request.Request(url, data=data, headers=headers, method='POST')
        
        # 发送请求并获取响应
        with urllib.request.urlopen(req) as response:
            status_code = response.getcode()
            response_body = response.read().decode('utf-8')
            
            print(f"Response Status Code: {status_code}")
            
            if status_code == 200:
                datasource_info = json.loads(response_body)
                print(f"数据源创建成功！")
                print(f"数据源ID: {datasource_info['id']}")
                print(f"数据源名称: {datasource_info['name']}")
                print(f"数据源类型: {datasource_info['type']}")
                return datasource_info['id']
            else:
                print(f"数据源创建失败: {response_body}")
                return None
    except Exception as e:
        print(f"创建数据源时出错: {e}")
        return None

# 2. 创建ETL任务
def create_etl_task(datasource_id):
    print("\n2. 创建ETL任务...")
    # ETL任务配置
    task_config = {
        "dataSourceId": datasource_id,
        "sourceTableName": "land_parcel",
        "targetTableName": "land_parcel",
        "description": "土地数据迁移任务"
    }

    try:
        # 准备请求数据
        url = "http://localhost:8082/api/tasks"
        data = json.dumps(task_config).encode('utf-8')
        headers = {
            'Content-Type': 'application/json',
            'Content-Length': str(len(data))
        }
        
        # 创建请求
        req = urllib.request.Request(url, data=data, headers=headers, method='POST')
        
        # 发送请求并获取响应
        with urllib.request.urlopen(req) as response:
            status_code = response.getcode()
            response_body = response.read().decode('utf-8')
            
            print(f"Response Status Code: {status_code}")
            
            if status_code == 200:
                task_info = json.loads(response_body)
                print(f"ETL任务创建成功！")
                print(f"任务ID: {task_info['taskId']}")
                print(f"任务状态: {task_info['status']}")
                return task_info['taskId']
            else:
                print(f"ETL任务创建失败: {response_body}")
                return None
    except Exception as e:
        print(f"创建ETL任务时出错: {e}")
        return None

# 3. 监控任务执行情况
def monitor_task(task_id):
    print(f"\n3. 监控任务执行情况 (任务ID: {task_id})...")
    
    # 监控任务状态，直到任务完成或失败
    for i in range(30):  # 最多监控30次，每次间隔10秒
        try:
            # 准备请求数据
            url = f"http://localhost:8082/api/tasks/task-id/{task_id}"
            headers = {
                'Content-Type': 'application/json'
            }
            
            # 创建请求
            req = urllib.request.Request(url, headers=headers, method='GET')
            
            # 发送请求并获取响应
            with urllib.request.urlopen(req) as response:
                status_code = response.getcode()
                response_body = response.read().decode('utf-8')
                
                if status_code == 200:
                    task_info = json.loads(response_body)
                    print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - 任务状态: {task_info['status']}")
                    print(f"  总分片数: {task_info['totalShards']}, 已完成分片数: {task_info['completedShards']}")
                    
                    # 如果任务状态为completed或failed，退出循环
                    if task_info['status'] in ['completed', 'failed']:
                        print("任务已完成或失败，退出监控")
                        return task_info
        except Exception as e:
            print(f"监控任务时出错: {e}")
        
        # 等待10秒后再次查询
        time.sleep(10)
    
    print("监控超时，任务可能仍在执行中")
    return None

# 4. 验证任务执行结果
def verify_task_result(task_info):
    print("\n4. 验证任务执行结果...")
    
    if task_info is None:
        print("无法验证任务结果，任务信息为空")
        return False
    
    print(f"任务ID: {task_info['taskId']}")
    print(f"最终状态: {task_info['status']}")
    print(f"总分片数: {task_info['totalShards']}")
    print(f"已完成分片数: {task_info['completedShards']}")
    
    # 验证分片数是否一致
    if task_info['totalShards'] == task_info['completedShards']:
        print("✅ 分片数验证通过：总分片数与已完成分片数一致")
        return True
    else:
        print("❌ 分片数验证失败：总分片数与已完成分片数不一致")
        return False

# 执行测试流程
if __name__ == "__main__":
    print("开始执行ETL流程测试...")
    print("=" * 60)
    
    # 1. 创建数据源
    datasource_id = create_datasource()
    if datasource_id is None:
        print("数据源创建失败，测试流程终止")
        exit(1)
    
    # 2. 创建ETL任务
    task_id = create_etl_task(datasource_id)
    if task_id is None:
        print("ETL任务创建失败，测试流程终止")
        exit(1)
    
    # 3. 监控任务执行情况
    task_info = monitor_task(task_id)
    if task_info is None:
        print("无法获取任务信息，测试流程终止")
        exit(1)
    
    # 4. 验证任务执行结果（分片数）
    shard_result = verify_task_result(task_info)
    
    # 5. 输出测试结果
    print("\n--- 测试结果总结 ---")
    print("=" * 60)
    print("系统测试ETL流程测试报告")
    print(f"测试时间: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)
    
    # 汇总测试结果
    all_passed = shard_result and task_info['status'] == 'completed'
    
    print(f"\n1. 数据源创建: ✅ 成功")
    print(f"2. 任务创建: ✅ 成功")
    print(f"3. 任务状态: {'✅ 成功' if task_info['status'] == 'completed' else f'❌ 失败 ({task_info['status']})'}")
    print(f"4. 分片数验证: {'✅ 成功' if shard_result else '❌ 失败'}")
    
    print("\n" + "=" * 60)
    
    if all_passed:
        print("🎉 测试通过：ETL流程执行成功")
        print("✅ 任务状态更新为成功完成")
        print("✅ 分片数验证通过：完成分片数=总分片数")
        exit(0)
    else:
        print("❌ 测试失败：部分测试项未通过")
        exit(1)