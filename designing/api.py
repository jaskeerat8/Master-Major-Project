# Importing Libraries
import uvicorn
from typing import List, Union
from fastapi import FastAPI, Query
from typing_extensions import Annotated
from neo4j import GraphDatabase


# Creating Session for Neo4j
URI = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
clean_database = "clean"
processed_database = "processed"
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))


# API for Neo4j
app = FastAPI()
@app.get("/")
async def hello():
    with GraphDatabase.driver(URI, auth=(username, password)) as driver:
        try:
            driver.verify_connectivity()
            return "Welcome to Neo4j"
        except Exception as e:
            return f"Failed to Connect: {e}"

@app.get("/get_info")
async def get_info(block_list: Annotated[Union[List[int], None], Query()] = None, transaction_list: Annotated[Union[List[str], None], Query()] = None):
    with neo4j_driver.session(database=clean_database) as session:
        if((block_list is not None) and (transaction_list is not None)):
            return "Pass only one parameter at a time"
        elif(block_list is not None):
            block_query = """
            MATCH (n:Block)
            WHERE n.number IN $block_list
            RETURN COLLECT(properties(n)) AS blocks
            """
            result = session.run(block_query, block_list=block_list)
            return result.data()[0]["blocks"]
        elif(transaction_list is not None):
            transaction_query = """
            MATCH (n:Transaction)
            WHERE n.txid IN $transaction_list
            RETURN COLLECT(properties(n)) AS transactions
            """
            result = session.run(transaction_query, transaction_list=transaction_list)
            return result.data()[0]["transactions"]

@app.get("/get_data")
async def get_data(block_list: Annotated[Union[List[int], None], Query()] = None, transaction_list: Annotated[Union[List[str], None], Query()] = None):
    with neo4j_driver.session(database=processed_database) as session:
        if((block_list is None) & (transaction_list is None)):
            return "Please pass a parameter"
        else:
            where_clauses = []
            if(block_list is not None):
                where_clauses.append("block.number IN $block_list")
            if(transaction_list is not None):
                where_clauses.append("transaction.txid IN $transaction_list")
            data_query = """MATCH (transaction)-[:INCLUDED_IN]->(block) {} RETURN COLLECT(properties(transaction)) AS transactions"""
            if(len(where_clauses) > 0):
                data_query = data_query.format("WHERE " + " AND ".join(where_clauses))
            result = session.run(data_query, block_list=block_list, transaction_list=transaction_list)
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
        result = session.run(transaction_query, transaction_id=transaction_id)
        return result.data()[0]["transaction_detail"]


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
