# Importing Libraries
import json, time
import pandas as pd
import numpy as np
from scipy import stats
from datetime import datetime, timedelta
from confluent_kafka import Consumer
from neo4j import GraphDatabase
from sklearn.neighbors import LocalOutlierFactor
from sklearn.preprocessing import StandardScaler
pd.set_option("display.max_colwidth", 30)

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
        for id in transaction_ids:
            alert_query = """
            MATCH (transaction:Transaction {id: $txid})
            CALL apoc.lock.nodes([transaction])
            WITH transaction
            SET transaction.alert = 1
            RETURN transaction
            """
            session.run(alert_query, txid=id)
            #print(f"Raised alert for Transaction: {id}")
    return True


# Statistical Analysis
point_analysis_threshold = 4
point_df = pd.DataFrame(columns=["txid", "value", "timestamp", "alert"])
multi_variate_threshold = 50
multi_df = pd.DataFrame(columns=["destination_address", "txid", "timestamp"])
lof_neighbors = 2
lof_df = pd.DataFrame(columns=["txid", "value", "timestamp"])

while True:
    message = consumer.poll(5)
    try:
        if (message is not None):
            transaction = json.loads(message.value().decode("utf-8"))

            # Point Analysis
            point_df = point_df.loc[point_df["timestamp"] >= datetime.now() - timedelta(seconds=3)]
            new_record = {"txid": transaction["txid"], "value": transaction["value"], "timestamp": datetime.now(), "alert": 0}
            point_df = pd.concat([point_df, pd.DataFrame([new_record])], ignore_index=True)
            point_df = point_df.reset_index(drop=True)
            z_scores = np.abs(stats.zscore(point_df["value"]))
            point_df["score"] = z_scores
            point_df.sort_values(by=["alert", "score"], ascending=False, inplace=True)
            point_df["alert"] = point_df["score"].apply(lambda x: 1 if x >= point_analysis_threshold else 0)
            raise_alert(point_df[point_df["alert"] == 1]["txid"])
            #print(point_df[["txid", "value", "score", "alert"]])

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
            #print(grouped_df[["destination_address", "count", "alert"]])

            # LOF
            lof_df = lof_df.loc[lof_df["timestamp"] >= datetime.now() - timedelta(seconds=10)]
            new_record = {"txid": transaction["txid"], "value": transaction["value"], "timestamp": datetime.now()}
            lof_df = pd.concat([lof_df, pd.DataFrame([new_record])], ignore_index=True)
            lof_df = lof_df.reset_index(drop=True)

            if(len(lof_df) >= lof_neighbors):
                values = lof_df["value"].values.reshape(-1, 1)
                scaler = StandardScaler()
                values_scaled = scaler.fit_transform(values)
                lof = LocalOutlierFactor(n_neighbors=lof_neighbors, contamination=0.1)
                lof_df["alert"] = lof.fit_predict(values_scaled)
                raise_alert(lof_df[lof_df["alert"] == -1]["txid"])
                print(lof_df[["txid", "value", "alert"]])
            else:
                pass
    except json.decoder.JSONDecodeError as e:
        print(f"Waiting For Data: {e}")
