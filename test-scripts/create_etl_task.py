import urllib.request
import urllib.parse
import json

# ETL任务配置
task_config = {
    "dataSourceId": 5,  # 数据源ID，从之前的响应中获取
    "sourceTableName": "land_parcel",  # 源表名，根据测试文档
    "targetTableName": "land_parcel",  # 目标表名，与源表名保持一致
    "description": "土地数据迁移任务"  # 任务描述
}

# 发送POST请求
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
        print(f"Response Content: {response_body}")
except Exception as e:
    print(f"Error: {e}")
