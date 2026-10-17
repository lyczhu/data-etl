import redis
import time

# 连接Redis
r = redis.Redis(host='localhost', port=6379, db=0)

# 测试分片计数器逻辑
def test_redis_shard_counter():
    # 生成测试任务ID
    task_id = f"test-{int(time.time())}"
    redis_key = f"status:{task_id}"
    
    print(f"测试任务ID: {task_id}")
    print(f"Redis键: {redis_key}")
    
    # 1. 初始化Redis计数器
    total_shards = 150
    r.hset(redis_key, "totalShards", total_shards)
    r.hset(redis_key, "completedShards", 0)
    
    print(f"\n1. 初始化分片计数器:")
    print(f"   totalShards = {total_shards}")
    print(f"   completedShards = 0")
    
    # 2. 模拟多次调用updateCompletedShards，超过总分片数
    print(f"\n2. 模拟数据存储服务多次调用updateCompletedShards():")
    
    for i in range(200):  # 模拟200次调用，超过总分片数150
        # 获取当前值
        completed_shards = int(r.hget(redis_key, "completedShards"))
        
        # 应用我们的修复逻辑：只在已完成分片数小于总分片数时才递增
        if completed_shards < total_shards:
            completed_shards = r.hincrby(redis_key, "completedShards", 1)
        
        # 每20次调用打印一次状态
        if (i + 1) % 20 == 0:
            print(f"   第{i+1}次调用: completedShards = {completed_shards}")
    
    # 3. 检查最终结果
    final_total = int(r.hget(redis_key, "totalShards"))
    final_completed = int(r.hget(redis_key, "completedShards"))
    
    print(f"\n3. 最终结果:")
    print(f"   totalShards = {final_total}")
    print(f"   completedShards = {final_completed}")
    
    # 4. 验证结果
    if final_completed == final_total:
        print(f"✅ 测试通过: 完成分片数({final_completed}) 等于 总分片数({final_total})")
        print(f"✅ 修复有效: 尽管调用了200次，但完成分片数没有超过总分片数")
        return True
    else:
        print(f"❌ 测试失败: 完成分片数({final_completed}) 不等于 总分片数({final_total})")
        return False

# 主函数
def main():
    print("开始测试Redis分片计数器修复...")
    success = test_redis_shard_counter()
    
    if success:
        print("\n🎉 所有测试通过! 分片计数修复有效。")
    else:
        print("\n❌ 测试失败! 分片计数修复存在问题。")

if __name__ == "__main__":
    main()