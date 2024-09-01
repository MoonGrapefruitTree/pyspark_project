import redis
import time
# 连接到本地Redis实例
r = redis.Redis(host='localhost', port=6379, db=0,decode_responses=True)

# 获取所有配置项
config = r.config_get()
for key, value in config.items():
    print(f"{key}: {value}")
# 设置自己需要的内容
# r.config_set('maxmemory', '256mb')  # 设置最大内存为256MB

# 插入数据
r.setex('key', 10, 'value')  # 设置键'key'，值'value'，10秒后过期
# 读取数据
print(r.get("key"))
time.sleep(10)
print(r.get("key"))
