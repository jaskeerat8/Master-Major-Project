# Importing Libraries
import supervised_analysis
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
processed_database = configurations["neo4j"]["processed_database"]
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))

def neo4j_processed(session, transaction):
    # Transaction Node
    block_transaction_query = """
    MERGE (block:Block {number: $block_number})
    MERGE (transaction:Transaction {txid: $txid})
    SET transaction.block_number = $block_number,
        transaction.time = $time,
        transaction.total_bitcoin_transacted = $total_bitcoin_transacted
    WITH transaction, block
    MERGE (transaction)-[:INCLUDED_IN]->(block)
    """
    session.run(block_transaction_query, txid=transaction["txid"], block_number=transaction["block_number"],
                time=transaction["time"], total_bitcoin_transacted=transaction["total_bitcoin_transacted"]
    )

    # VOUT Address Node
    for destination in transaction["vout"]:
        destination_transaction_id = str(transaction["txid"]) + "_" + str(destination["n"])
        destination_address_transaction_query = """
        CREATE (subtransaction:SubTransaction {txid: $sub_txid})
        SET subtransaction.address = $destination_address,
            subtransaction.value = $value,
            subtransaction.is_utxo = $is_utxo,
            subtransaction.Transaction_type = $Transaction_type
        with subtransaction
        MERGE (transaction:Transaction {txid: $txid})
        MERGE (transaction)-[:OUTPUTS]->(subtransaction)
        """
        session.run(destination_address_transaction_query, sub_txid=destination_transaction_id, destination_address=destination.get("scriptPubKey", {}).get("address", None),
                    value=destination["value"], is_utxo=destination["is_utxo"], Transaction_type=destination["Transaction_type"], txid=transaction["txid"]
        )

    # VIN Address Node
    for source in transaction["vin"]:
        illegal_probability = supervised_analysis.prediction(transaction, source)
        source_address_transaction_query = """
        MERGE (subtransaction:SubTransaction {txid: $sub_txid})
        SET subtransaction.address = $source_address,
            subtransaction.value = $value,
            subtransaction.Transaction_type = $Transaction_type,
            subtransaction.supervised_alert = $supervised_alert,
            subtransaction.supervised_alert_probability = $supervised_alert_probability
        with subtransaction
        MERGE (transaction:Transaction {txid: $txid})
        MERGE (subtransaction)-[:INPUTS]->(transaction)
        """
        session.run(source_address_transaction_query, sub_txid=source["txid"], source_address=source["address"], value=source["value"],
                    Transaction_type=source["Transaction_type"], txid=transaction["txid"], supervised_alert=(1 if(illegal_probability >= 0.5) else 0),
                    supervised_alert_probability=illegal_probability
        )
    return True


def consume_messages(topic, config, consumer_id):
    consumer = Consumer(config)
    consumer.subscribe([topic])
    with neo4j_driver.session(database=processed_database) as session:
        while True:
            message = consumer.poll(5)
            try:
                if (message is not None):
                    transaction = json.loads(message.value().decode("utf-8"))
                    print("Transaction Received In Consumer:", consumer_id)
                    neo4j_processed(session, transaction)
            except json.decoder.JSONDecodeError as e:
                print(f"Waiting For Data: {e}")

if __name__ == "__main__":
    consumer_topic = "block_transactions"
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "transaction_processed_consumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "max.poll.interval.ms": 1000000
    }

    thread1 = threading.Thread(target=consume_messages, args=(consumer_topic, consumer_config, 1))
    thread2 = threading.Thread(target=consume_messages, args=(consumer_topic, consumer_config, 2))
    thread1.start()
    thread2.start()
