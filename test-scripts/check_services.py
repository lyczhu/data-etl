import urllib.request
import json

# 访问注册中心API，获取已注册的服务列表
try:
    # 准备请求数据
    url = "http://localhost:8761/eureka/apps"
    headers = {
        'Accept': 'application/json'
    }
    
    # 创建请求
    req = urllib.request.Request(url, headers=headers, method='GET')
    
    # 发送请求并获取响应
    with urllib.request.urlopen(req) as response:
        status_code = response.getcode()
        response_body = response.read().decode('utf-8')
        
        print(f"Response Status Code: {status_code}")
        
        # 解析响应
        if status_code == 200:
            eureka_data = json.loads(response_body)
            
            if 'applications' in eureka_data and 'application' in eureka_data['applications']:
                applications = eureka_data['applications']['application']
                print(f"已注册的服务数量: {len(applications)}")
                
                for app in applications:
                    print(f"\n服务名称: {app['name']}")
                    print(f"  实例数量: {len(app['instance'])}")
                    for instance in app['instance']:
                        print(f"  - 实例ID: {instance['instanceId']}")
                        print(f"    状态: {instance['status']}")
                        print(f"    主机名: {instance['hostName']}")
                        print(f"    IP地址: {instance['ipAddr']}")
                        print(f"    端口: {instance['port']['$']}")
            else:
                print("没有找到已注册的服务")
except Exception as e:
    print(f"Error: {e}")
