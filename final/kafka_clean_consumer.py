# Importing Libraries
import yaml
import json
import threading
from neo4j import GraphDatabase
from confluent_kafka import Consumer

# Reading Configurations
with open("configurations.yaml") as f:
    configurations = yaml.safe_load(f)

# Creating Session for Neo4j
URI = configurations["neo4j"]["uri"]
username = configurations["neo4j"]["username"]
password = configurations["neo4j"]["password"]
clean_database = configurations["neo4j"]["clean_database"]
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))

def neo4j_clean(session, transaction):
    # VIN Addresses
    vin_list = []
    for source in transaction["vin"]:
        vin_dict = {"address": source["address"], "txid": source["txid"], "value": source["value"], "Transaction_type": source["Transaction_type"]}
        vin_list.append(vin_dict)

    # VOUT Addresses
    vout_list = []
    for destination in transaction["vout"]:
        vout_dict = {"address": destination.get("scriptPubKey", {}).get("address", None), "is_utxo": destination["is_utxo"], "value": destination["value"], "Transaction_type": destination["Transaction_type"]}
        vout_list.append(vout_dict)

    # Transaction Node
    block_transaction_query = """
    CREATE (transaction:Transaction {txid: $txid})
    SET transaction.block_number = $block_number,
        transaction.time = $time,
        transaction.fee = $fee,
        transaction.size = $size,
        transaction.weight = $weight,
        transaction.total_bitcoin_transacted = $total_bitcoin_transacted,
        transaction.vin = $vin,
        transaction.vout = $vout
    WITH transaction
    MATCH (block:Block {number: $block_number})
    MERGE (transaction)-[:INCLUDED_IN]->(block)
    """
    session.run(block_transaction_query, txid=transaction["txid"], block_number=transaction["block_number"], time=transaction["time"],
                fee=transaction["fee"], size=transaction["size"], weight=transaction["weight"],
                total_bitcoin_transacted=transaction["total_bitcoin_transacted"], vin=str(vin_list), vout=str(vout_list)
    )

    # VIN Address Node
    for source in transaction["vin"]:
        source_address_transaction_query = """
        MATCH (source:Address {address: $source_address})
        CREATE (subtransaction:SubTransaction {subtxid: $sub_txid})
        SET subtransaction.address = $source_address,
            subtransaction.value = $value,
            subtransaction.Transaction_type = $Transaction_type
        with source, subtransaction
        MATCH (transaction:Transaction {txid: $txid})
        MERGE (source)-[:PERFORMS]->(subtransaction)
        MERGE (subtransaction)-[:FOR]->(transaction)
        """
        session.run(source_address_transaction_query, source_address=source["address"], sub_txid=source["txid"], value=source["value"],
                    Transaction_type=source["Transaction_type"], txid=transaction["txid"]
        )

    # VOUT Address Node
    for destination in transaction["vout"]:
        destination_transaction_id = str(transaction["txid"]) + "_" + str(destination["n"])
        destination_address = destination.get("scriptPubKey", {}).get("address", None)

        destination_address_transaction_query = """
            MERGE (subtransaction:SubTransaction {subtxid: $sub_txid})
            SET subtransaction.address = $destination_address,
                subtransaction.value = $value,
                subtransaction.is_utxo = $is_utxo,
                subtransaction.Transaction_type = $Transaction_type
            with subtransaction
            MATCH (transaction:Transaction {txid: $txid})
            MERGE (transaction)-[:FROM]->(subtransaction)
            """

        if(destination_address is not None):
            destination_address_transaction_query = destination_address_transaction_query + """
            MERGE (destination:Address {address: $destination_address})
            MERGE (subtransaction)-[:RECEIVES]->(destination)
            """
        else:
            print("Bitcoin Used Up, No Destination Address Encountered")

        session.run(destination_address_transaction_query, destination_address=destination_address, sub_txid=destination_transaction_id,
                    value=destination["value"], is_utxo=destination["is_utxo"], Transaction_type=destination["Transaction_type"], txid=transaction["txid"]
        )
    return True


def consume_messages(topic, config, consumer_id):
    consumer = Consumer(config)
    consumer.subscribe([topic])
    with neo4j_driver.session(database=clean_database) as session:
        while True:
            message = consumer.poll(5)
            try:
                if (message is not None):
                    transaction = json.loads(message.value().decode("utf-8"))
                    print("Transaction Received In Consumer:", consumer_id)
                    neo4j_clean(session, transaction)
            except json.decoder.JSONDecodeError as e:
                print(f"Waiting For Data: {e}")

if __name__ == "__main__":
    consumer_topic = "block_transactions"
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "transaction_clean_consumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "max.poll.interval.ms": 1000000
    }

    thread1 = threading.Thread(target=consume_messages, args=(consumer_topic, consumer_config, 1))
    thread2 = threading.Thread(target=consume_messages, args=(consumer_topic, consumer_config, 2))
    thread3 = threading.Thread(target=consume_messages, args=(consumer_topic, consumer_config, 3))
    thread4 = threading.Thread(target=consume_messages, args=(consumer_topic, consumer_config, 4))
    thread1.start()
    thread2.start()
    thread3.start()
    thread4.start()
