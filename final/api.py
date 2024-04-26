# Importing Libraries
import supervised_analysis
import uvicorn
import requests
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from neo4j import GraphDatabase

# Bitcoin RPC API Server
rpc_api = "http://192.168.19.148:5001/query_data"

# Creating Session for Neo4j
URI = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
clean_database = "clean"
processed_database = "processed"
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))


# API for Neo4j
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)

@app.get("/")
async def hello():
    with GraphDatabase.driver(URI, auth=(username, password)) as driver:
        try:
            driver.verify_connectivity()
            return "Welcome to Neo4j"
        except Exception as e:
            return f"Failed to Connect: {e}"

@app.get("/get_info")
async def get_info(block_number: str = None, transaction_id: str = None):
    with neo4j_driver.session(database=clean_database) as session:
        if((block_number is not None) and (transaction_id is not None)):
            return "Pass only one parameter at a time"
        elif(block_number is not None):
            block_query = """
            MATCH (n:Block)
            WHERE n.number = $block_number
            RETURN properties(n) AS block_data
            """
            result = session.run(block_query, block_number=int(block_number))
            return result.data()[0]["block_data"]
        elif(transaction_id is not None):
            transaction_query = """
            MATCH (n:Transaction)
            WHERE n.txid = $transaction_id
            RETURN properties(n) AS transaction
            """
            result = session.run(transaction_query, transaction_id=transaction_id)
            return result.data()[0]["transaction"]

@app.get("/get_alert_data")
async def get_alert_data(block_number: str = None):
    with neo4j_driver.session(database=processed_database) as session:
        if(block_number is None):
            data_query = """
            MATCH (transaction:Transaction)
            WHERE transaction.unsupervised_anomaly = 1
            RETURN collect(transaction.txid) AS transactions;
            """
            result = session.run(data_query)
        else:
            data_query = """
            MATCH (transaction:Transaction {block_number: $block_number})
            WHERE transaction.unsupervised_anomaly = 1
            RETURN collect(transaction.txid) AS transactions;
            """
            result = session.run(data_query, block_number=int(block_number))
        return result.data()[0]["transactions"]

@app.get("/get_transaction")
async def get_transaction(transaction_id: str):
    with neo4j_driver.session(database=processed_database) as session:
        transaction_query = """
        MATCH (transaction:Transaction {txid: $transaction_id})-[:INCLUDED_IN]->(block)
        OPTIONAL MATCH (transaction)-[:OUTPUTS]->(outputSub:SubTransaction)
        OPTIONAL MATCH (inputSub:SubTransaction)-[:INPUTS]->(transaction)
        WITH transaction, block,
            COLLECT(DISTINCT {
                txid: inputSub.txid,
                address: inputSub.address,
                value: inputSub.value,
                Transaction_type: inputSub.Transaction_type,
                supervised_alert: inputSub.supervised_alert,
                supervised_alert_probability: inputSub.supervised_alert_probability
            }) AS vin,
            COLLECT(DISTINCT {
                n: SPLIT(outputSub.txid, "_")[1],
                address: outputSub.address,
                value: outputSub.value,
                is_utxo: outputSub.is_utxo,
                Transaction_type: outputSub.Transaction_type
            }) AS vout
        RETURN {
            txid: transaction.txid,
            block_number: transaction.block_number,
            vin: vin,
            vout: vout
        } AS transaction_detail
        """
        try:
            result = session.run(transaction_query, transaction_id=transaction_id)
            return result.data()[0]["transaction_detail"]
        except:
            result = requests.get(rpc_api, params={"query": transaction_id})
            result = result.json()
            payload = {"block_number": result.get("height", None), "txid": result.get("txid", None)}

            vin_list = []
            for source in result["vin"]:
                illegal_probability = supervised_analysis.prediction(result, source)
                vin_list.append({
                    "supervised_alert": (1 if(illegal_probability >= 0.5) else 0),
                    "address": source.get("address", None),
                    "Transaction_type": source.get("Transaction_type", None),
                    "txid": source.get("txid", None),
                    "value": source.get("value", None),
                    "supervised_alert_probability": illegal_probability
                })

            vout_list = []
            for destination in result["vout"]:
                vout_list.append({
                    "is_utxo": destination.get("is_utxo", None),
                    "address": destination.get("scriptPubKey", {}).get("address", None),
                    "Transaction_type": destination.get("Transaction_type", None),
                    "n": destination.get("n", None),
                    "value": destination.get("value", None)
                })

            payload["vin"] = vin_list
            payload["vout"] = vout_list
            return payload


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
