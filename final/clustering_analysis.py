# Importing Libraries
import yaml
import json
import pandas as pd
from neo4j import GraphDatabase
from confluent_kafka import Consumer
from datetime import datetime, timedelta
from sklearn.cluster import DBSCAN
pd.set_option('display.max_columns', None)

# Reading Configurations
with open("configurations.yaml") as f:
    configurations = yaml.safe_load(f)

# Creating Session for Neo4j
URI = configurations["neo4j"]["uri"]
username = configurations["neo4j"]["username"]
password = configurations["neo4j"]["password"]
processed_database = configurations["neo4j"]["processed_database"]
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))

# Raise Anomaly
def raise_alert(alert_df):

    global main_df
    with neo4j_driver.session(database=processed_database) as session:
        for index, row in alert_df.sort_values(["value", "influence", "in_degree"], ascending=[False, False, False]).iterrows():
            alert_query = """
            MERGE (transaction:Transaction {txid: $txid})
            SET transaction.unsupervised_anomaly = 1
            RETURN transaction
            """
            session.run(alert_query, txid=row["txid"])
            print("Raised Alert for Transaction:", row["txid"], "with value:", row["value"], "influence:", row["influence"], "and in_degree:", row["in_degree"])
            main_df.loc[main_df["txid"] == row["txid"], "raised_alert"] = 1
    return True

# Cluster Analysis
def cluster_analysis(cluster_df, block):
    features = ["value", "fee", "in_degree", "nu_out_degree", "balance", "influence", "z_score"]

    x = cluster_df[features]

    dbscan = DBSCAN(eps=configurations["dbscan"]["eps"], min_samples=configurations["dbscan"]["min_points"])
    clusters = dbscan.fit_predict(x)
    cluster_df["cluster"] = clusters

    raise_alert(cluster_df[(cluster_df["cluster"] == -1) & (cluster_df["raised_alert"] == 0) & (cluster_df["block"] == block)])
    return True


if __name__ == "__main__":
    # Main DataFrame
    main_df = pd.DataFrame(columns=["block", "txid", "value", "fee", "in_degree", "nu_out_degree", "balance", "influence", "z_score", "time", "raised_alert"])

    consumer_topic = "block_data"
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "clustering_analysis_consumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "max.poll.interval.ms": 1000000
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([consumer_topic])

    while True:
        message = consumer.poll(5)
        try:
            if (message is not None):
                json_data = json.loads(message.value().decode("utf-8"))
                main_df = main_df[main_df["time"] >= datetime.now() - timedelta(minutes=configurations["dbscan"]["time_window"] )]

                block_number = json_data["block_info"]["height"]
                print("\nNew Block Received:", block_number)

                transaction_df = pd.DataFrame()
                for transaction in json_data["transactions"][1:]:
                    transaction_df = transaction_df._append({"block": block_number, "txid": transaction["txid"], "value": float(transaction.get("receiver_total_received", 0)),
                    "fee": float(transaction["fee"]), "in_degree": transaction["in_degree"], "nu_out_degree": transaction["nu_out_degree"],
                    "balance": transaction["in_degree"] - transaction["nu_out_degree"],
                    "influence": transaction["in_degree"] / (transaction["in_degree"] + transaction["nu_out_degree"]),
                    "time": datetime.now(), "raised_alert": 0}, ignore_index=True)
                transaction_df["z_score"] = (transaction_df["value"] - transaction_df["value"].mean()) / transaction_df["value"].std()

                main_df = pd.concat([main_df, transaction_df])
                cluster_analysis(main_df, block_number)
        except json.decoder.JSONDecodeError as e:
            print(f"Waiting For Data: {e}")
