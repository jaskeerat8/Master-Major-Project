# Importing Libraries
import json
import pandas as pd
from datetime import datetime, timedelta
from confluent_kafka import Consumer
from neo4j import GraphDatabase
from sklearn.ensemble import IsolationForest
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", 20)


# Z Score Analysis
isolation_contamination = float(0.01)
df = pd.DataFrame(columns=["txid", "value", "timestamp", "alert", "raised_alert"])

# Creating Session for Neo4j
URI = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))

def raise_alert(transaction_df):
    global df
    with neo4j_driver.session(database="processed") as session:
        for index, row in transaction_df.iterrows():
            alert_query = """
            MATCH (transaction:Transaction {id: $txid})
            CALL apoc.lock.nodes([transaction])
            WITH transaction
            SET transaction.isolation_alert = 1
            RETURN transaction
            """
            session.run(alert_query, txid=row["txid"])
            df.loc[df["txid"] == row["txid"], "raised_alert"] = 1
            print(f"""Raised alert for Transaction: {row["txid"]} with value {row["value"]}""")
    return True


# Creating a Kafka Consumer instance
consumer_topic = "transaction_alerts"
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "isolation_forest_consumer",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True
})
consumer.subscribe([consumer_topic])

while True:
    message = consumer.poll(5000)
    try:
        if (message is not None):
            transaction = json.loads(message.value().decode("utf-8"))
            transaction_time = datetime.utcfromtimestamp(message.timestamp()[1] / 1000)

            df = df.loc[df["timestamp"] >= transaction_time - timedelta(minutes=10)]
            new_record = {"txid": transaction["txid"], "value": transaction["value"], "timestamp": transaction_time, "alert": 0, "raised_alert": 0}
            df = pd.concat([df, pd.DataFrame([new_record])], ignore_index=True)

            model = IsolationForest(n_estimators=100, max_samples="auto", contamination=isolation_contamination, random_state=42)
            model.fit(df[["value"]])
            df["score"] = model.decision_function(df[["value"]])
            df["alert"] = model.predict(df[["value"]])
            df.sort_values(by=["alert", "value"], ascending=[True, False], inplace=True)
            raise_alert(df[(df["alert"] == -1) & (df["raised_alert"] == 0)][["txid", "value"]])

    except json.decoder.JSONDecodeError as e:
        print(f"Waiting For Data: {e}")
