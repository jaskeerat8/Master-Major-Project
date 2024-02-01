# Importing Libraries
import time
import json
import requests
from requests.auth import HTTPBasicAuth
from confluent_kafka import Producer
from neo4j import GraphDatabase

# Creating Session for Neo4j
URI = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))
with GraphDatabase.driver(URI, auth=(username, password)) as driver:
    try:
        driver.verify_connectivity()
        print("Connected to Neo4j")
    except Exception as e:
        print("Failed to Connect:", e)

def insert_block_data(block_data):
    for db in ["clean", "processed"]:
        with neo4j_driver.session(database=db) as session:
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
            session.run(block_query, block_number=block_data["height"], hash=block_data["hash"], height=block_data["height"],
                        merkleroot=block_data["merkleroot"], time=block_data["time"], mediantime=block_data["mediantime"],
                        difficulty=block_data["difficulty"], nTx=block_data["nTx"], previousblockhash=block_data["previousblockhash"],
                        strippedsize=block_data["strippedsize"], size=block_data["size"], weight=block_data["weight"])
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
    static_data = r"C:\Users\jaske\Downloads\data_output.json"
    with open(static_data, "r") as file:
        json_data = json.load(file)

    insert_block_data(json_data["block_info"])
    transaction_count = 0
    for transaction in json_data["transactions"]:
        transaction["block_number"] = json_data["block_info"]["height"]
        transaction["time"] = json_data["block_info"]["time"]
        kafka_produce_transactions(transaction_producer, transaction_topic, transaction)
        transaction_count = transaction_count + 1
    print(f"{transaction_count} Transactions Sent for Processing")

else:
    username = "duck"
    password = "duck2"
    rpc_server_url = "https://ducks.loca.lt/api/data"

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
                # Sending Block Data
                insert_block_data(json_data["block_info"])

                # Sending Transaction Data
                transaction_count = 0
                for transaction in json_data["transactions"]:
                    transaction["block_number"] = new_block_number
                    transaction["time"] = json_data["block_info"]["time"]
                    kafka_produce_transactions(transaction_producer, transaction_topic, transaction)
                    transaction_count = transaction_count + 1

                print(f"Found New Block: {new_block_number} with {transaction_count} Transactions")
                rpc_server = 1
                block_number = new_block_number
        except Exception as e:
            print(f"An error occurred: {e}")
            rpc_server = 0

        if(rpc_server == 1):
            print("Waiting for 5 min before checking for new Data")
            time.sleep(300)
        else:
            print("Waiting for 1 min before checking for new Data")
            time.sleep(60)
