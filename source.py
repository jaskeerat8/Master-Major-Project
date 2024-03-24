# Importing Libraries
import os, csv
import time, json
import random, requests
from datetime import datetime
from requests.auth import HTTPBasicAuth
from confluent_kafka import Producer
from neo4j import GraphDatabase

# Source Logs
def log_data(data):
    log_file = "logs/source_custom_logs.csv"
    if not os.path.exists(log_file.split("/")[0]):
        os.makedirs(log_file.split("/")[0])
    if not os.path.exists(log_file):
        with open(log_file, "w", newline="") as file:
            writer = csv.writer(file)
            writer.writerow(["block", "creation_time", "receiver_time"])
    with open(log_file, "a", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(data)

# Creating Session for Neo4j
URI = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
clean_database = "clean"
processed_database = "processed"
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))
with GraphDatabase.driver(URI, auth=(username, password)) as driver:
    try:
        driver.verify_connectivity()
        print("Connected to Neo4j")
    except Exception as e:
        print("Failed to Connect:", e)

def insert_block_data(block_data):
    with neo4j_driver.session(database=processed_database) as session:
        block_query = """MERGE (block:Block {number: $block_number})"""
        session.run(block_query, block_number=block_data["height"])

    with neo4j_driver.session(database=clean_database) as session:
        block_query = """
        MERGE (block:Block {number: $block_number})
        SET block.hash = $hash,
            block.height = $height,
            block.merkleroot = $merkleroot,
            block.time = $time,
            block.mediantime = $mediantime,
            block.difficulty = $difficulty,
            block.nTx = $nTx,
            block.previousblockhash = $previousblockhash,
            block.strippedsize = $strippedsize,
            block.size = $size,
            block.weight = $weight
        """
        session.run(block_query, block_number=block_data["height"], hash=block_data["hash"], height=block_data["height"], merkleroot=block_data["merkleroot"],
            time=block_data["time"], mediantime=block_data["mediantime"], difficulty=block_data["difficulty"], nTx=block_data["nTx"],
            previousblockhash=block_data["previousblockhash"], strippedsize=block_data["strippedsize"], size=block_data["size"], weight=block_data["weight"]
        )
    print(f"""Block {block_data["height"]} Data Inserted""")
    return True

# Creating a Kafka producer instance
transaction_topic = "block_transactions"
transaction_producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "transaction_producer"
})
def kafka_produce_transactions(kafka_producer, topic, transaction_message):
    kafka_producer.produce(topic=topic, value=json.dumps(transaction_message).encode("utf-8"))
    kafka_producer.flush()
    return True


# Connecting to Data Source
source_flag = 1
if(source_flag):

    static_data = r"C:\Users\jaske\Downloads\new_data.json"
    with open(static_data, "r") as file:
        json_data = json.load(file)

    while True:
        insert_block_data(json_data["block_info"])
        transaction_count = 0
        for transaction in json_data["transactions"]:
            transaction["block_number"] = json_data["block_info"]["height"]
            transaction["time"] = json_data["block_info"]["time"]
            transaction["vin"] = [{k: v for k, v in data.items() if k != "txinwitness"} for data in transaction["vin"]]
            transaction.pop("hex", None)
            kafka_produce_transactions(transaction_producer, transaction_topic, transaction)
            transaction_count = transaction_count + 1
        print(f"{transaction_count} Transactions Sent for Processing")
        wait_time = random.choice([5, 6, 7, 8])
        print(f"Waiting For {wait_time} minutes")
        time.sleep(wait_time * 60)

else:
    username = "duck"
    password = "duck2"
    rpc_server_url = "http://192.168.224.148:5001/api/new_data"

    rpc_server = 1
    block_number = float("-inf")
    while True:
        try:
            response = requests.get(rpc_server_url, auth=HTTPBasicAuth(username, password))
            json_data = response.json()
            new_block_number = json_data["block_info"]["height"]
            if(new_block_number == block_number):
                rpc_server = 0
                print(f"Incurred Old Block: {new_block_number}")
            else:
                print(f"\nFound New Block: {new_block_number} with {len(json_data['transactions'])} Transactions")

                # Adding File Received Time For Logs
                log_data([new_block_number, json_data["processed_at"], str(datetime.now())])

                # Sending Block Data
                insert_block_data(json_data["block_info"])

                # Sending Transaction Data
                for transaction in json_data["transactions"]:
                    transaction["block_number"] = new_block_number
                    transaction["time"] = json_data["block_info"]["time"]
                    transaction["vin"] = [{k: v for k, v in data.items() if k != "txinwitness"} for data in transaction["vin"]]
                    transaction.pop("hex", None)
                    try:
                        kafka_produce_transactions(transaction_producer, transaction_topic, transaction)
                    except Exception as e:
                        print(transaction, "\n")
                        print("Size:", len(json.dumps(transaction).encode("utf-8")))

                # Setting Flags
                rpc_server = 1
                block_number = new_block_number
        except Exception as e:
            print(f"An error occurred: {e}")
            rpc_server = 0

        if(rpc_server == 1):
            print("Waiting for 1 min before checking for new Data")
            time.sleep(60)
        else:
            print("Waiting for 30 sec before trying again")
            time.sleep(30)
