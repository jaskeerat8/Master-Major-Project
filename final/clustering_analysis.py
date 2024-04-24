# Importing Libraries
import json
import time
import pandas as pd
from neo4j import GraphDatabase
from confluent_kafka import Consumer
from datetime import datetime, date, timedelta

# Creating Session for Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
processed_database = "processed"
neo4j_driver = GraphDatabase.driver(uri, auth=(username, password))

if __name__ == "__main__":
    consumer_topic = "block_transactions"
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "clustering_consumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "max.poll.interval.ms": 1000000
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([consumer_topic])

    with neo4j_driver.session(database=processed_database) as session:
        while True:
            message = consumer.poll(5)
            try:
                if (message is not None):
                    transaction = json.loads(message.value().decode("utf-8"))
                    print("Transaction Received In Consumer at:", datetime.utcfromtimestamp(message.timestamp()[1] / 1000))
            except json.decoder.JSONDecodeError as e:
                print(f"Waiting For Data: {e}")
