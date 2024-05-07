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
df = pd.DataFrame(columns=["address", "txid", "value", "timestamp", "raised_alert"])

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
            SET transaction.grouped_alert = 1
            RETURN transaction
            """
            session.run(alert_query, txid=t)
            df.loc[df["txid"] == t, "raised_alert"] = 1
            print(f"""Raised Grouped alert for Transaction: {t} part of {count_of_transactions} Transaction list""")
    return True


# Creating a Kafka Consumer instance
consumer_topic = "block_transactions"
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "multi_transaction_consumer",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True,
    "max.poll.interval.ms": 1000000
})
consumer.subscribe([consumer_topic])

count = 0
while True:
    message = consumer.poll(5)
    try:
        if (message is not None):
            transaction = json.loads(message.value().decode("utf-8"))
            transaction_time = datetime.utcfromtimestamp(message.timestamp()[1] / 1000)

            transaction_df = pd.DataFrame(columns=["address", "txid", "value", "timestamp", "raised_alert"])
            for vin in transaction["vin"]:
                transaction_df = transaction_df._append({"address": vin["address"], "txid": transaction["txid"], "value": vin["value"], "timestamp": transaction_time, "raised_alert": 0}, ignore_index=True)
            df = df.loc[df["timestamp"] >= transaction_time - timedelta(minutes=30)]
            df = pd.concat([df, transaction_df], ignore_index=True)

            grouped_df = df.groupby("address")["txid"].agg(set).reset_index()
            grouped_df["count"] = grouped_df["txid"].apply(len)
            grouped_df.columns = ["address", "transactions", "count"]
            grouped_df.sort_values(by=["count"], ascending=False, inplace=True)
            print(grouped_df, "\n")

            alert_transaction_list = set().union(*grouped_df[grouped_df["count"] >= transaction_threshold]["transactions"])
            number_of_transactions = len(alert_transaction_list)
            alert_transaction_list = list(alert_transaction_list - set(df[df["raised_alert"] == 1]["txid"]))
            raise_alert(alert_transaction_list, number_of_transactions)

    except json.decoder.JSONDecodeError as e:
        print(f"Waiting For Data: {e}")
