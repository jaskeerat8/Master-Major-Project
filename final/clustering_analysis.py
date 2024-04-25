# Importing Libraries
import json
import pandas as pd
from neo4j import GraphDatabase
from datetime import datetime, timedelta
from sklearn.cluster import DBSCAN
from sklearn.preprocessing import StandardScaler
from sklearn.decomposition import PCA

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
            main_df.loc[main_df["txid"] == row["txid"], "raised_alert"] = 1
            print(f"""Raised alert for Transaction: {row["txid"]} with value {row["value"]}""")
    return True

# Cluster Analysis
def cluster_analysis(cluster_df):
    features = ["value", "fee", "in_degree", "nu_out_degree", "balance", "influence", "z_score"]

    x = cluster_df[features]
    # x_scaled = StandardScaler().fit_transform(x)
    # x_pca = PCA(n_components=3).fit_transform(x_scaled)

    dbscan = DBSCAN(eps=0.6, min_samples=8)
    clusters = dbscan.fit_predict(x)
    cluster_df["cluster"] = clusters

    raise_alert(cluster_df[(cluster_df["cluster"] == -1) & (cluster_df["raised_alert"] == 0)][["txid", "value"]])
    return True


if __name__ == "__main__":
    # Main DataFrame
    main_df = pd.DataFrame(columns=["txid", "value", "fee", "in_degree", "nu_out_degree", "balance", "influence", "z_score", "time", "raised_alert"])

    cluster_data_file = "cluster_data/data.json"
    block_number = float("-inf")
    while True:
        try:
            with open(cluster_data_file, "r") as json_file:
                json_data = json.load(json_file)

            if(block_number == json_data["block_info"]["height"]):
                continue
            else:
                main_df = main_df[main_df["time"] >= datetime.now() - timedelta(minutes=15)]

                transaction_df = pd.DataFrame()
                for transaction in json_data["transactions"][1:]:
                    transaction_df = transaction_df._append({"txid": transaction["txid"], "value": float(transaction["receiver_total_received"]),
                    "fee": float(transaction["fee"]), "in_degree": transaction["in_degree"], "nu_out_degree": transaction["nu_out_degree"],
                    "balance": transaction["in_degree"] - transaction["nu_out_degree"],
                    "influence": transaction["in_degree"] / (transaction["in_degree"] + transaction["nu_out_degree"]),
                    "time": datetime.now(), "raised_alert": 0}, ignore_index=True)
                transaction_df["z_score"] = (transaction_df["value"] - transaction_df["value"].mean()) / transaction_df["value"].std()

                main_df = pd.concat([main_df, transaction_df])
                block_number = json_data["block_info"]["height"]
                print("\n", block_number)
                cluster_analysis(main_df)
        except json.decoder.JSONDecodeError as e:
            continue
