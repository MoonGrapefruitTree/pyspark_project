from confluent_kafka.admin import AdminClient, NewTopic

conf = {'bootstrap.servers': '10.38.218.43:9094,10.38.218.43:9092,10.38.218.43:9093',  # 连接 Kafka Broker 的地址
        'security.protocol': 'PLAINTEXT'}
topic_name = "my_new_topic"
# 创建客户端
admin_client = AdminClient(conf)

admin_client.delete_topics([topic_name])
new_topic = NewTopic(topic=topic_name, num_partitions=1)
fs = admin_client.create_topics([new_topic])
for topic, f in fs.items():
        try:
                f.result()  # The result itself is None
                print("Topic {} created".format(topic))
        except Exception as e:
                print("Failed to create topic {}: {}".format(topic, e))

cluster_metadata = admin_client.list_topics(timeout=10)
print(list(cluster_metadata.topics.keys()))