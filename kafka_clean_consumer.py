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
    # Transaction Node
    block_transaction_query = """
    MERGE (transaction:Transaction {txid: $txid})
    SET transaction.block_number = $block_number,
        transaction.time = $time,
        transaction.fee = $fee,
        transaction.size = $size,
        transaction.weight = $weight,
        transaction.receiver_mean_received = $receiver_mean_received,
        transaction.receiver_total_received = $receiver_total_received,
        transaction.total_bitcoin_transacted = $total_bitcoin_transacted
    WITH transaction
    MATCH (block:Block {number: $block_number})
    MERGE (transaction)-[:INCLUDED_IN]->(block)
    """
    session.run(block_transaction_query, txid=transaction["txid"], block_number=transaction["block_number"], time=transaction["time"],
                fee=transaction["fee"], size=transaction["size"], weight=transaction["weight"], receiver_mean_received=transaction["receiver_mean_received"],
                receiver_total_received=transaction["receiver_total_received"], total_bitcoin_transacted=transaction["total_bitcoin_transacted"]
    )

    # VIN Address Node
    vin_address = []
    for source in transaction["vin"]:
        source_address = source["address"]
        vin_address.append(source_address)
        source_address_transaction_query = """
        MERGE (source:Address {address: $source_address})
        MERGE (subtransaction:SubTransaction {txid: $sub_txid})
        SET subtransaction.address = $source_address,
            subtransaction.value = $value,
            subtransaction.Transaction_type = $Transaction_type,
            subtransaction.sender_mean_sent = $sender_mean_sent,
            subtransaction.sender_total_sent = $sender_total_sent
        with source, subtransaction
        MATCH (transaction:Transaction {txid: $txid})
        MERGE (source)-[:PERFORMS]->(subtransaction)
        MERGE (subtransaction)-[:FOR]->(transaction)
        """
        session.run(source_address_transaction_query, source_address=source_address, sub_txid=source["txid"], value=source["value"],
                    Transaction_type=source["Transaction_type"], sender_mean_sent=source["sender_mean_sent"],
                    sender_total_sent=source["sender_total_sent"], txid=transaction["txid"]
        )

    # VOUT Address Node
    vout_address = []
    for destination in transaction["vout"]:
        destination_transaction_id = str(transaction["txid"]) + "_" + str(destination["n"])

        if(all(op not in destination["scriptPubKey"]["asm"] for op in ("OP_RETURN", "OP_CHECKMULTISIG"))):
            destination_address = destination["scriptPubKey"]["address"]
            vout_address.append(destination_address)
            destination_address_transaction_query = """
            MERGE (destination:Address {address: $destination_address})
            MERGE (subtransaction:SubTransaction {txid: $sub_txid})
            SET subtransaction.address = $destination_address,
                subtransaction.value = $value,
                subtransaction.is_utxo = $is_utxo,
                subtransaction.Transaction_type = $Transaction_type
            with destination, subtransaction
            MATCH (transaction:Transaction {txid: $txid})
            MERGE (transaction)-[:FROM]->(subtransaction)
            MERGE (subtransaction)-[:RECEIVES]->(destination)
            """
            session.run(destination_address_transaction_query, destination_address=destination_address, sub_txid=destination_transaction_id,
                        value=destination["value"], is_utxo=destination["is_utxo"], Transaction_type=destination["Transaction_type"], txid=transaction["txid"]
            )
        else:
            print("Bitcoin Used Up, No Destination SubTransaction Encountered")
            destination_transaction_query = """
            MERGE (subtransaction:SubTransaction {txid: $sub_txid})
            SET subtransaction.value = $value,
                subtransaction.is_utxo = $is_utxo,
                subtransaction.Transaction_type = $Transaction_type
            with subtransaction
            MATCH (transaction:Transaction {txid: $txid})
            MERGE (transaction)-[:FROM]->(subtransaction)
            """
            session.run(destination_transaction_query, sub_txid=destination_transaction_id, value=destination["value"],
                        is_utxo=destination["is_utxo"], Transaction_type=destination["Transaction_type"], txid=transaction["txid"]
            )

    # Update Transaction Node
    update_transaction = """
    MATCH (transaction:Transaction {txid: $txid})
    SET transaction.source = $source,
        transaction.destination = $destination
    RETURN transaction
    """
    session.run(update_transaction, txid=transaction["txid"], source=vin_address, destination=vout_address)
    return True


def consume_messages(topic, config, consumer_id):
    consumer = Consumer(config)
    consumer.subscribe([topic])
    with neo4j_driver.session(database=clean_database) as session:
        while True:
            message = consumer.poll(5000)
            try:
                if (message is not None):
                    transaction = json.loads(message.value().decode("utf-8"))
                    if(any(data.get("address") == "Coinbase Transaction" for data in transaction["vin"])):
                        pass
                    else:
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
