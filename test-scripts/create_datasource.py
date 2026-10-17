import urllib.request
import urllib.parse
import json

# 数据源配置
datasource_config = {
    "name": "shenzhen_postgres",
    "type": "postgresql",
    "url": "jdbc:postgresql://localhost:5432/shenzhen_db",
    "username": "postgres",
    "password": "shenzhen2026",
    "driverClassName": "org.postgresql.Driver",
    "description": "深圳数据库"
}

# 发送POST请求
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
        print(f"Response Content: {response_body}")
except Exception as e:
    print(f"Error: {e}")
