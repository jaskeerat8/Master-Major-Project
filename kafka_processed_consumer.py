# Importing Libraries
import json
from confluent_kafka import Consumer, Producer
from threading import Thread
from neo4j import GraphDatabase

# Creating a Kafka producer instance
statistical_alert_topic = "transaction_statistical_alerts"
statistical_alert_producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "statistical_alert_producer"
})
supervised_alert_topic = "transaction_supervised_alerts"
supervised_alert_producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "supervised_alert_producer"
})


# Creating Session for Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
processed_database = "processed"
neo4j_driver = GraphDatabase.driver(uri, auth=(username, password))

def neo4j_processed(transaction):
    with neo4j_driver.session(database=processed_database) as session:
        # VIN Address Node
        transaction_value = 0
        vin_address = []
        for source in transaction["vin"]:
            transaction_value = transaction_value + source["value"]
            vin_address.append((source["address"], source["txid"], source["value"]))

        # VOUT Address Node
        utxo_dict = {0: [], 1: []}
        vout_address = []
        for destination in transaction["vout"]:
            utxo_dict[0].append(destination["n"]) if destination["is_utxo"] == 0 else utxo_dict[1].append(destination["n"])
            destination_transaction_id = str(transaction["txid"]) + "_" + str(destination["n"])
            if(all(op not in destination["scriptPubKey"]["asm"] for op in ("OP_RETURN", "OP_CHECKMULTISIG"))):
                destination_address = destination["scriptPubKey"]["address"]
                vout_address.append((destination_address, destination_transaction_id, destination["value"]))
            else:
                vout_address.append((None, destination_transaction_id, destination["value"]))

        # Transaction Node
        block_transaction_query = """
        MERGE (transaction:Transaction {id: $txid})
        SET transaction.block_number = $block_number,
            transaction.time = $time,
            transaction.value = $value,
            transaction.fee = $fee,
            transaction.source = $source,
            transaction.destination = $destination,
            transaction.utxo = $utxo
        WITH transaction
        MATCH (block:Block {number: $block_number})
        MERGE (transaction)-[:INCLUDED_IN]->(block)
        """
        session.run(block_transaction_query, txid=transaction["txid"], block_number=transaction["block_number"], time=transaction["time"],
                    value=transaction_value, fee=transaction["fee"], source=str(vin_address), destination=str(vout_address), utxo=str(utxo_dict)
        )

        # Sending Data For Alert Check
        transaction_statistical_info = {
            "txid": transaction["txid"], "destination": vout_address, "value": transaction_value, "time": transaction["time"],
            "in_degree": transaction["in_degree"], "out_degree": transaction["out_degree"], "total_degree": transaction["total_degree"],
            "nu_out_degree": transaction["nu_out_degree"], "block_number": transaction["block_number"]
        }
        kafka_produce_alerts(statistical_alert_producer, statistical_alert_topic, transaction_statistical_info)

        transaction_supervised_info = {
            "txid": transaction["txid"], "value": transaction_value, "fee": transaction["fee"], "time": transaction["time"],
            "in_degree": transaction["in_degree"], "out_degree": transaction["out_degree"], "total_degree": transaction["total_degree"],
            "nu_out_degree": transaction["nu_out_degree"], "block_number": transaction["block_number"]
        }
        kafka_produce_alerts(supervised_alert_producer, supervised_alert_topic, transaction_supervised_info)
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
                    neo4j_processed(transaction)
        except json.decoder.JSONDecodeError as e:
            print(f"Waiting For Data: {e}")

def main():
    consumer_topic = "block_transactions"
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "transaction_processed_consumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True
    }

    consumer1 = Consumer(consumer_config)
    consumer2 = Consumer(consumer_config)

    consumer1.subscribe([consumer_topic])
    consumer2.subscribe([consumer_topic])

    thread1 = Thread(target=consume_messages, args=(consumer1, 1))
    thread2 = Thread(target=consume_messages, args=(consumer2, 2))
    try:
        thread1.start()
        thread2.start()

        thread1.join()
        thread2.join()
    except KeyboardInterrupt:
        pass
    finally:
        consumer1.close()
        consumer2.close()

if __name__ == "__main__":
    main()
