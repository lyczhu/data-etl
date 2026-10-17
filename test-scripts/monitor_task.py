import urllib.request
import urllib.parse
import json
import time

# 任务ID，从之前的响应中获取
task_id = "ec728ace-b0ec-4de8-b8ee-784ac20cd5fb"

# 监控任务状态
try:
    for i in range(10):
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
            
            # 解析响应
            task_info = json.loads(response_body)
            
            print(f"{time.strftime('%Y-%m-%d %H:%M:%S')} - 任务状态: {task_info['status']}")
            print(f"  总分片数: {task_info['totalShards']}, 已完成分片数: {task_info['completedShards']}")
            print(f"  开始时间: {task_info['startTime']}, 结束时间: {task_info['endTime']}")
            print(f"  错误信息: {task_info['errorMessage']}")
            
            # 如果任务状态为completed或failed，退出循环
            if task_info['status'] in ['completed', 'failed']:
                print("任务已完成或失败，退出监控")
                break
        
        # 等待5秒后再次查询
        time.sleep(5)
except Exception as e:
    print(f"Error: {e}")
