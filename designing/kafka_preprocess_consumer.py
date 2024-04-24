# Importing Libraries
import json
from confluent_kafka import Consumer, Producer
from threading import Thread
from neo4j import GraphDatabase

# Creating a Kafka producer instance
alert_topic = "transaction_alerts"
alert_producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "alert_producer"
})

# Creating Session for Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
clean_database = "clean"
processed_database = "processed"
neo4j_driver = GraphDatabase.driver(uri, auth=(username, password))

def neo4j_clean(transaction):
    with neo4j_driver.session(database=clean_database) as session:
        # Transaction Node
        transaction_query = """
        MERGE (transaction:Transaction {id: $txid})
        SET transaction.time = $time,
            transaction.hash = $hash,
            transaction.size = $size,
            transaction.weight = $weight,
            transaction.in_degree = $in_degree,
            transaction.out_degree = $out_degree,
            transaction.total_degree = $total_degree,
            transaction.nu_out_degree = $nu_out_degree,
            transaction.block_number = $block_number
        WITH transaction
        MATCH (block:Block {number: $block_number})
        MERGE (transaction)-[:INCLUDED_IN]->(block)
        """
        session.run(transaction_query, txid=transaction["txid"], time=transaction["time"], hash=transaction["hash"], size=transaction["size"],
                    weight=transaction["weight"], in_degree=transaction["in_degree"], out_degree=transaction["out_degree"],
                    total_degree=transaction["total_degree"], nu_out_degree=transaction["nu_out_degree"], block_number=transaction["block_number"]
        )

        # VIN Address Node
        transaction_value = 0
        vin_address = []
        for source in transaction["vin"]:
            transaction_value = transaction_value + source["value"]
            source_address = source["address"]
            vin_address.append(source_address)

            source_address_transaction_query = """
            MERGE (source:Address {address: $source_address})
            MERGE (subtransaction:SubTransaction {id: $sub_txid})
            SET subtransaction.address = $source_address,
                subtransaction.value = $value
            with source, subtransaction
            MATCH (transaction:Transaction {id: $txid})
            MERGE (source)-[:PERFORMS]->(subtransaction)
            MERGE (subtransaction)-[:FOR]->(transaction)
            """
            session.run(source_address_transaction_query, txid=transaction["txid"], sub_txid=source["txid"], value=source["value"],
                        source_address=source_address
            )

        # VOUT Address Node
        utxo_dict = {0: [], 1: []}
        vout_address = []
        for destination in transaction["vout"]:
            utxo_dict[0].append(destination["n"]) if destination["is_utxo"] == 0 else utxo_dict[1].append(destination["n"])
            destination_transaction_id = str(transaction["txid"]) + "_" + str(destination["n"])

            if(all(op not in destination["scriptPubKey"]["asm"] for op in ("OP_RETURN", "OP_CHECKMULTISIG"))):
                destination_address = destination["scriptPubKey"]["address"]
                vout_address.append(destination_address)

                destination_address_transaction_query = """
                MERGE (destination:Address {address: $destination_address})
                MERGE (subtransaction:SubTransaction {id: $sub_txid})
                SET subtransaction.address = $destination_address,
                    subtransaction.value = $value,
                    subtransaction.is_utxo = $is_utxo
                with destination, subtransaction
                MATCH (transaction:Transaction {id: $txid})
                MERGE (transaction)-[:FROM]->(subtransaction)
                MERGE (subtransaction)-[:RECEIVES]->(destination)
                """
                session.run(destination_address_transaction_query, txid=transaction["txid"], sub_txid=destination_transaction_id,
                            destination_address=destination_address, value=destination["value"], is_utxo=destination["is_utxo"]
                )
            else:
                print("Bitcoin Used, No Destination SubTransaction Encountered")
                destination_transaction_query = """
                MERGE (subtransaction:SubTransaction {id: $sub_txid})
                SET subtransaction.value = $value,
                    subtransaction.is_utxo = $is_utxo
                with subtransaction
                MATCH (transaction:Transaction {id: $txid})
                MERGE (transaction)-[:FROM]->(subtransaction)
                """
                session.run(destination_transaction_query, txid=transaction["txid"], sub_txid=destination_transaction_id,
                            value=destination["value"], is_utxo=destination["is_utxo"]
                )

        # Update Transaction Node
        update_transaction = """
        MATCH (transaction:Transaction {id: $txid})
        CALL apoc.lock.nodes([transaction])
        WITH transaction
        SET transaction.value = $value,
            transaction.source = $source,
            transaction.destination = $destination,
            transaction.utxo = $utxo
        RETURN transaction
        """
        session.run(update_transaction, txid=transaction["txid"], value=transaction_value, source=vin_address,
                    destination=vout_address, utxo=str(utxo_dict)
        )
    processed_transaction_info = {"source": vin_address, "destination": vout_address, "txid": transaction["txid"], "value": transaction_value,
                "time": transaction["time"], "in_degree": transaction["in_degree"], "out_degree": transaction["out_degree"],
                "total_degree": transaction["total_degree"], "nu_out_degree": transaction["nu_out_degree"], "block_number": transaction["block_number"]}
    return processed_transaction_info


def neo4j_processed(transaction):
    with neo4j_driver.session(database=processed_database) as session:
        block_transaction_query = """
        MERGE (transaction:Transaction {id: $txid})
        SET transaction.timestamp = $timestamp,
            transaction.value = $value,
            transaction.alert = 0,
            transaction.in_degree = $in_degree,
            transaction.out_degree = $out_degree,
            transaction.total_degree = $total_degree,
            transaction.nu_out_degree = $nu_out_degree,
            transaction.block_number = $block_number
        with transaction
        MATCH (block:Block {number: $block_number})
        MERGE (transaction)-[:INCLUDED_IN]->(block)
        """
        session.run(block_transaction_query, txid=transaction["txid"], timestamp=transaction["time"], value=transaction["value"],
                    in_degree=transaction["in_degree"], out_degree=transaction["out_degree"], total_degree=transaction["total_degree"],
                    nu_out_degree=transaction["nu_out_degree"], block_number=transaction["block_number"]
        )
        for source in transaction["source"]:
            source_address_query = """
            MERGE (source:Address {address: $source_address})
            WITH source
            MATCH (transaction:Transaction {id: $txid})
            MERGE (source)-[:PERFORMS]->(transaction)
            """
            session.run(source_address_query, source_address=source, txid=transaction["txid"])
        for destination in transaction["destination"]:
            destination_address_query = """
            MERGE (destination:Address {address: $destination_address})
            WITH destination
            MATCH (transaction:Transaction {id: $txid})
            MERGE (transaction)-[:RECEIVES]->(destination)
            """
            session.run(destination_address_query, destination_address=destination, txid=transaction["txid"])

        # Sending Data For Alert Check
        kafka_produce_alerts(alert_producer, alert_topic, transaction)
        return True


def kafka_produce_alerts(kafka_producer, topic, transaction_message):
    kafka_producer.produce(topic=topic, value=json.dumps(transaction_message).encode("utf-8"))
    kafka_producer.flush()
    return True


def consume_messages(consumer, consumer_id):
    while True:
        message = consumer.poll(5)
        try:
            if (message is not None):
                transaction = json.loads(message.value().decode("utf-8"))
                if(any(data.get("address") == "Coinbase Transaction" for data in transaction["vin"])):
                    pass
                else:
                    print("Transaction Received In Consumer:", consumer_id)
                    processed_transaction = neo4j_clean(transaction)
                    neo4j_processed(processed_transaction)
        except json.decoder.JSONDecodeError as e:
            print(f"Waiting For Data: {e}")


def main():
    consumer_topic = "block_transactions"
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "transaction_consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True
    }

    consumer1 = Consumer(consumer_config)
    consumer2 = Consumer(consumer_config)
    consumer3 = Consumer(consumer_config)
    consumer4 = Consumer(consumer_config)

    consumer1.subscribe([consumer_topic])
    consumer2.subscribe([consumer_topic])
    consumer3.subscribe([consumer_topic])
    consumer4.subscribe([consumer_topic])

    thread1 = Thread(target=consume_messages, args=(consumer1, 1))
    thread2 = Thread(target=consume_messages, args=(consumer2, 2))
    thread3 = Thread(target=consume_messages, args=(consumer3, 3))
    thread4 = Thread(target=consume_messages, args=(consumer4, 4))

    try:
        thread1.start()
        thread2.start()
        thread3.start()
        thread4.start()

        thread1.join()
        thread2.join()
        thread3.join()
        thread4.join()
    except KeyboardInterrupt:
        pass
    finally:
        consumer1.close()
        consumer2.close()
        consumer3.close()
        consumer4.close()

if __name__ == "__main__":
    main()
