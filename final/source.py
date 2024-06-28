# Importing Libraries
#import source_decryption
import os, csv, yaml
import time, json
import random, requests
from datetime import datetime
from requests.auth import HTTPBasicAuth
from confluent_kafka import Producer
from neo4j import GraphDatabase

# Source Data
def log_data(log, rpc_api_data):
    # Writing Log
    log_file = "log/data_log.csv"
    if not os.path.exists(log_file.split("/")[0]):
        os.makedirs(log_file.split("/")[0])
    if not os.path.exists(log_file):
        with open(log_file, "w", newline="") as log_f:
            writer = csv.writer(log_f)
            writer.writerow(["block", "transactions", "mining_time", "creation_time", "received_time"])
    with open(log_file, "a", newline="") as log_f:
        writer = csv.writer(log_f)
        writer.writerow(log)

    # Saving Data for Analysis
    api_data_file = f"rpc_api_data/{str(rpc_api_data['block_info']['height'])}.json"
    if not os.path.exists(api_data_file.split("/")[0]):
        os.makedirs(api_data_file.split("/")[0])
    with open(api_data_file, "w") as api_file:
        json.dump(rpc_api_data, api_file, indent=4)
    return True


# Reading Configurations
with open("configurations.yaml") as f:
    configurations = yaml.safe_load(f)

# Creating Session for Neo4j
URI = configurations["neo4j"]["uri"]
username = configurations["neo4j"]["username"]
password = configurations["neo4j"]["password"]
clean_database = configurations["neo4j"]["clean_database"]
processed_database = configurations["neo4j"]["processed_database"]
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
block_topic = "block_data"
source_producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "source_producer",
    "message.max.bytes": 31457280
})
def kafka_produce_transactions(kafka_producer, topic, transaction_message):
    kafka_producer.produce(topic=topic, value=json.dumps(transaction_message).encode("utf-8"))
    kafka_producer.flush()
    return True
def kafka_produce_block(kafka_producer, topic, transactions):
    kafka_producer.produce(topic=topic, value=json.dumps(transactions).encode("utf-8"))
    kafka_producer.flush()
    return True


# Connecting to Data Source
source_flag = 0
if(source_flag == 0):
    # folder_path = r"C:\Users\jaske\Downloads\data"
    # files = os.listdir(folder_path)

    #for f in files:
    # with open(folder_path + f"\{f}", "r") as file:
    #     json_data = json.load(file)

    with open(r"C:\Users\jaske\Downloads\rpc_api_data\842539.json", "r") as file:
        json_data = json.load(file)

    insert_block_data(json_data["block_info"])
    kafka_produce_block(source_producer, block_topic, json_data)

    transaction_count = 0
    for transaction in json_data["transactions"][1:]:
        transaction["block_number"] = json_data["block_info"]["height"]
        transaction["time"] = json_data["block_info"]["time"]
        transaction["vin"] = [{k: v for k, v in data.items() if k != "txinwitness"} for data in transaction["vin"]]
        transaction.pop("hex", None)
        kafka_produce_transactions(source_producer, transaction_topic, transaction)
        transaction_count = transaction_count + 1
    print(f"{transaction_count} Transactions Sent for Processing")
    wait_time = random.choice([5, 6, 7, 8])
    print(f"Waiting For {wait_time} minutes\n")
    time.sleep(wait_time * 60)
else:
    username = configurations["rpc"]["username"]
    password = configurations["rpc"]["password"]
    rpc_server_url = f"""http://{configurations["rpc"]["ip"]}:{configurations["rpc"]["port"]}/api/new_data"""

    rpc_server = 1
    block_number = float("-inf")
    while True:
        try:
            response = requests.get(rpc_server_url, auth=HTTPBasicAuth(username, password))
            json_data = response.json()

            new_block_number = json_data["block_info"]["height"]
            print(f"{new_block_number} Block Data Decrypted")
            if(new_block_number == block_number):
                rpc_server = 0
                print(f"Incurred Old Block: {new_block_number}")
            else:
                print(f"\nFound New Block: {new_block_number} with {len(json_data['transactions'])} Transactions")

                # Adding File Received Time For Logs
                log_data([new_block_number, len(json_data["transactions"]), str(datetime.fromtimestamp(json_data["block_info"]["time"])),
                          str(datetime.fromisoformat(json_data["processed_at"])), str(datetime.now())], json_data)

                # Sending Block Data
                insert_block_data(json_data["block_info"])

                # Sending Data for Cluster Analysis
                kafka_produce_block(source_producer, block_topic, json_data)

                # Sending Transaction Data
                for transaction in json_data["transactions"][1:]:
                    transaction["block_number"] = new_block_number
                    transaction["time"] = json_data["block_info"]["time"]
                    transaction["vin"] = [{k: v for k, v in data.items() if k != "txinwitness"} for data in transaction["vin"]]
                    transaction.pop("hex", None)
                    try:
                        kafka_produce_transactions(source_producer, transaction_topic, transaction)
                    except:
                        print(transaction, "\nSize:", len(json.dumps(transaction).encode("utf-8")))

                # Setting Flags
                rpc_server = 1
                block_number = new_block_number
        except Exception as e:
            print(f"An error occurred: {e} not found")
            rpc_server = 0

        if(rpc_server == 1):
            print("Waiting for 1 min before checking for new Data")
            time.sleep(60)
        else:
            print("Waiting for 30 sec before trying again")
            time.sleep(30)
