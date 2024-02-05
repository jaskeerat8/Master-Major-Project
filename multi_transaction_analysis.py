# Importing Libraries
import json
import pandas as pd
from datetime import datetime, timedelta
from confluent_kafka import Consumer
from neo4j import GraphDatabase
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", 30)


# Multi Transaction Analysis
transaction_threshold = 50
df = pd.DataFrame(columns=["txid", "destination_address", "timestamp", "raised_alert"])

# Creating Session for Neo4j
URI = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))

def raise_alert(transaction_list, count_of_transactions):
    global df
    with neo4j_driver.session(database="processed") as session:
        for t in transaction_list:
            alert_query = """
            MATCH (transaction:Transaction {id: $txid})
            CALL apoc.lock.nodes([transaction])
            WITH transaction
            SET transaction.grouped_alert = 1
            RETURN transaction
            """
            session.run(alert_query, txid=t)
            df.loc[df["txid"] == t, "raised_alert"] = 1
            print(f"""Raised Grouped alert for Transaction: {t} part of {count_of_transactions} Transaction list""")
    return True


# Creating a Kafka Consumer instance
consumer_topic = "transaction_alerts"
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "multi_transaction_consumer",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True
})
consumer.subscribe([consumer_topic])
count = 0
while True:
    message = consumer.poll(5000)
    try:
        if (message is not None):
            transaction = json.loads(message.value().decode("utf-8"))
            transaction_time = datetime.utcfromtimestamp(message.timestamp()[1] / 1000)
            transaction_df = pd.DataFrame([{"txid": transaction["txid"], "destination_address": transaction["destination"], "timestamp": transaction_time, "raised_alert": 0}])
            transaction_df = transaction_df.explode("destination_address", ignore_index=True)
            count = count + 1

            df = df.loc[df["timestamp"] >= transaction_time - timedelta(minutes=30)]
            df = pd.concat([df, transaction_df], ignore_index=True)

            grouped_df = df.groupby("destination_address")["txid"].agg(set).reset_index()
            grouped_df["count"] = grouped_df["txid"].apply(len)
            grouped_df.columns = ["destination_address", "transactions", "count"]
            grouped_df.sort_values(by=["count"], ascending=False, inplace=True)

            alert_transaction_list = set().union(*grouped_df[grouped_df["count"] >= transaction_threshold]["transactions"])
            number_of_transactions = len(alert_transaction_list)
            alert_transaction_list = list(alert_transaction_list - set(df[df["raised_alert"] == 1]["txid"]))
            raise_alert(alert_transaction_list, number_of_transactions)

    except json.decoder.JSONDecodeError as e:
        print(f"Waiting For Data: {e}")
