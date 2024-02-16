# Importing Libraries
import json, time
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timedelta
from confluent_kafka import Consumer
from neo4j import GraphDatabase
from sklearn.ensemble import IsolationForest
pd.set_option("display.max_columns", None)
pd.set_option("display.max_colwidth", 20)

# Creating a Kafka Consumer instance
consumer_topic = "transaction_alerts"
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "alert_consumer",
    "auto.offset.reset": "latest",
    "enable.auto.commit": True
})
consumer.subscribe([consumer_topic])


# Creating Session for Neo4j
URI = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))

def raise_alert(transaction_ids):
    with neo4j_driver.session(database="processed") as session:
        for txid in transaction_ids:
            alert_query = """
            MATCH (transaction:Transaction {id: $txid})
            CALL apoc.lock.nodes([transaction])
            WITH transaction
            SET transaction.alert = 1
            RETURN transaction
            """
            session.run(alert_query, txid=txid)
            #print(f"Raised alert for Transaction: {id}")
    return True


# Statistical Analysis
point_analysis_df = pd.DataFrame(columns=["timestamp_range", "comparison"])
zscore_threshold = 3
zscore_df = pd.DataFrame(columns=["txid", "value", "timestamp", "alert"])
multi_variate_threshold = 50
multi_df = pd.DataFrame(columns=["destination_address", "txid", "timestamp"])
isolation_contamination = float(0.01)
iso_df = pd.DataFrame(columns=["txid", "value", "timestamp"])

while True:
    message = consumer.poll(5)
    try:
        if (message is not None):
            transaction = json.loads(message.value().decode("utf-8"))

            # Point Analysis
            end_time = (datetime.utcfromtimestamp(message.timestamp()[1] / 1000) + timedelta(minutes=1)).replace(second=0, microsecond=0)
            start_time = (end_time - timedelta(minutes=10))
            timestamp_range = str(start_time) + " / " + str(end_time)
            if timestamp_range not in point_analysis_df["timestamp_range"].values:
                new_record = {"timestamp_range": timestamp_range, "comparison": 1}
                point_analysis_df = pd.concat([point_analysis_df, pd.DataFrame([new_record])], ignore_index=True)

            comparison = point_analysis_df.loc[point_analysis_df["timestamp_range"] == timestamp_range, "comparison"].iloc[0]
            print("Our Transaction:", transaction["value"], "Comparison:", comparison)
            if(comparison > transaction["value"]):
                raise_alert([transaction["txid"]])

            # Z-Score Analysis
            message_time = datetime.utcfromtimestamp(message.timestamp()[1] / 1000)
            zscore_df = zscore_df.loc[zscore_df["timestamp"] >= message_time - timedelta(seconds=10)]
            new_record = {"txid": transaction["txid"], "value": transaction["value"], "timestamp": message_time, "alert": 0}
            zscore_df = pd.concat([zscore_df, pd.DataFrame([new_record])], ignore_index=True)
            zscore_df = zscore_df.reset_index(drop=True)
            zscore_df["score"] = np.abs(stats.zscore(zscore_df["value"]))
            zscore_df.sort_values(by=["alert", "score"], ascending=False, inplace=True)
            zscore_df["alert"] = zscore_df["score"].apply(lambda x: 1 if x >= zscore_threshold else 0)
            raise_alert(zscore_df[zscore_df["alert"] == 1]["txid"])
            print(zscore_df)

            # Multi Variate Analysis
            multi_df = multi_df.loc[multi_df["timestamp"] >= datetime.now() - timedelta(seconds=30)]
            for address in transaction["destination"]:
                new_record = {"destination_address": address, "txid": transaction["txid"], "timestamp": datetime.now()}
                multi_df = pd.concat([multi_df, pd.DataFrame([new_record])], ignore_index=True)
                multi_df = multi_df.reset_index(drop=True)
            grouped_df = multi_df.groupby("destination_address").agg({"txid": lambda x: list(x), "timestamp": "count"}).reset_index()
            grouped_df.rename(columns={"timestamp": "count"}, inplace=True)
            grouped_df.sort_values(by=["count"], ascending=False, inplace=True)
            grouped_df["alert"] = grouped_df["count"].apply(lambda x: 1 if x >= multi_variate_threshold else 0)
            raise_alert(sum(grouped_df[grouped_df["alert"] == 1]["txid"], []))
            print(grouped_df[["destination_address", "count", "alert"]])

            # Isolation Forest
            iso_df = iso_df.loc[iso_df["timestamp"] >= datetime.now() - timedelta(seconds=10)]
            new_record = {"txid": transaction["txid"], "value": transaction["value"], "timestamp": datetime.now()}
            iso_df = pd.concat([iso_df, pd.DataFrame([new_record])], ignore_index=True)
            iso_df = iso_df.reset_index(drop=True)

            model = IsolationForest(n_estimators=100, max_samples="auto", contamination=isolation_contamination, random_state=42)
            model.fit(iso_df[["value"]])
            iso_df["score"] = model.decision_function(iso_df[["value"]])
            iso_df["alert"] = model.predict(iso_df[["value"]])
            raise_alert(iso_df[iso_df["alert"] == -1]["txid"])
            print(iso_df[["txid", "value", "alert"]])

    except json.decoder.JSONDecodeError as e:
        print(f"Waiting For Data: {e}")
