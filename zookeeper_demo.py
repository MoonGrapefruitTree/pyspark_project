from kazoo.client import KazooClient
import json

# 连接到 ZooKeeper 集群
zookeeper_hosts = ["localhost:2181"]  # 替换为你的 ZooKeeper 地址
zk = KazooClient(hosts=zookeeper_hosts)
zk.start()

# 从 ZooKeeper 获取 broker ID 列表
broker_ids = zk.get_children("/brokers/ids")

# 初始化一个列表来存储 broker 信息
broker_info = []

# 获取每个 broker 的主机信息
for broker_id in broker_ids:
    broker_data, _ = zk.get(f"/brokers/ids/{broker_id}")
    broker_info.append(broker_data.decode("utf-8"))

# 提取 Kafka 主机信息
kafka_hosts = []
for broker in broker_info:
    # 假设 broker 信息是 JSON 格式
    broker_json = json.loads(broker)
    host = broker_json.get("host")
    port = broker_json.get("port", 9092)  # 如果未指定端口，默认为 9092
    kafka_hosts.append(f"{host}:{port}")

print(kafka_hosts)

# 完成后停止 ZooKeeper 客户端
zk.stop()
