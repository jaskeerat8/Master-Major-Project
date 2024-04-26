# Importing Libraries
import json
import threading
from neo4j import GraphDatabase
from confluent_kafka import Consumer

# Creating Session for Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
clean_database = "clean"
neo4j_driver = GraphDatabase.driver(uri, auth=(username, password))

def neo4j_clean(session, transaction):
    # VIN Address Node
    vin_list = []
    for source in transaction["vin"]:
        vin_dict = {"address": source["address"], "txid": source["txid"], "value": source["value"], "Transaction_type": source["Transaction_type"]}
        vin_list.append(vin_dict)

    # VOUT Address Node
    vout_list = []
    for destination in transaction["vout"]:
        vout_dict = {"address": destination.get("scriptPubKey", {}).get("address"), "is_utxo": destination["is_utxo"], "value": destination["value"], "Transaction_type": destination["Transaction_type"]}
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
    thread1.start()
    thread2.start()
