# Importing Libraries
import time
from confluent_kafka.admin import AdminClient, NewTopic

# Delete Existing Kafka Topics
admin_client = AdminClient({"bootstrap.servers": "localhost:9092"})

# Deleting Topics
topics = admin_client.list_topics().topics.keys()
for topic in topics:
    if topic != "__consumer_offsets":
        admin_client.delete_topics([topic])
    time.sleep(2)
print("After Deleting Topics:", topics)

# Creating Topics
topics = [
    NewTopic(topic="block_transactions", num_partitions=4, replication_factor=0),
    NewTopic(topic="block_data", num_partitions=1, replication_factor=0)
]
created_topics = admin_client.create_topics(topics)
for topic, future in created_topics.items():
    print("Topic:", topic, "Created")
