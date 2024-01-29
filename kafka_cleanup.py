# Importing Libraries
import os
import shutil

# Clean the Kafka logs
logs = [r"C:\kafka\kafka_logs\server_logs", r"C:\kafka\kafka_logs\zookeeper_logs"]
for log in logs:
    shutil.rmtree(log)
    os.makedirs(log)
