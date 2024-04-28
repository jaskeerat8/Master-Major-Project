# Importing Libraries
import json
import pandas as pd
from neo4j import GraphDatabase
from confluent_kafka import Consumer
from datetime import datetime, timedelta
from sklearn.cluster import DBSCAN
pd.set_option('display.max_columns', None)

# Creating Session for Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
processed_database = "processed"
neo4j_driver = GraphDatabase.driver(uri, auth=(username, password))

# Raise Anomaly
def raise_alert(alert_df):
    global main_df
    with neo4j_driver.session(database=processed_database) as session:
        for index, row in alert_df.iterrows():
            alert_query = """
            MERGE (transaction:Transaction {txid: $txid})
            SET transaction.unsupervised_anomaly = 1
            RETURN transaction
            """
            session.run(alert_query, txid=row["txid"])
            print("Raised Alert for Transaction:", row["txid"], "with value:", row["value"], "and in_degree:", row["in_degree"])
            main_df.loc[main_df["txid"] == row["txid"], "raised_alert"] = 1
    return True

# Cluster Analysis
def cluster_analysis(cluster_df, block):
    features = ["value", "fee", "in_degree", "nu_out_degree", "balance", "influence", "z_score"]

    x = cluster_df[features]

    dbscan = DBSCAN(eps=0.6, min_samples=7)
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
                main_df = main_df[main_df["time"] >= datetime.now() - timedelta(minutes=15)]

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
