# Importing Libraries
import io
import json
import gzip
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
            return result.data()
        elif(transaction_list is not None):
            transaction_query = """
            MATCH (n:Transaction)
            WHERE n.id IN $transaction_list
            RETURN COLLECT(properties(n)) AS transactions
            """
            result = session.run(transaction_query, transaction_list=transaction_list)
            return result.data()

@app.get("/get_data")
async def get_data(block_list: Annotated[Union[List[int], None], Query()] = None, transaction_list: Annotated[Union[List[str], None], Query()] = None, start_timestamp: str = None, end_timestamp: str = None):
    with neo4j_driver.session(database=processed_database) as session:
        if((block_list is None) & (transaction_list is None)):
            return "Please pass a parameter"
        else:
            where_clauses = []
            if(block_list is not None):
                where_clauses.append("block.number IN $block_list")
            if(transaction_list is not None):
                where_clauses.append("transaction.id IN $transaction_list")
            if((start_timestamp is not None) & (end_timestamp is not None)):
                where_clauses.append("datetime($start_timestamp) <= transaction.time <= datetime($end_timestamp)")

            data_query = """MATCH (transaction)-[:INCLUDED_IN]->(block) {} RETURN COLLECT(properties(transaction)) AS transactions"""
            if(len(where_clauses) > 0):
                data_query = data_query.format("WHERE " + " AND ".join(where_clauses))

            result = session.run(data_query, block_list=block_list, transaction_list=transaction_list, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
            return result.data()

@app.post("/post_alert")
async def post_alert(transaction_list: Annotated[Union[List[str], None], Query()] = None):
    with neo4j_driver.session(database=processed_database) as session:
        if(len(transaction_list) > 0):
            for txid in transaction_list:
                alert_query = """
                MATCH (transaction:Transaction {id: $txid})
                CALL apoc.lock.nodes([transaction])
                WITH transaction
                SET transaction.supervised_alert = 1
                RETURN transaction
                """
                session.run(alert_query, txid=txid)
            return f"Raised Alerts for {transaction_list}"
        else:
            return f"Please send a List of Transactions"


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8000)
