# Importing Libraries
import time
import json, requests
import threading
import pandas as pd
from neo4j import GraphDatabase
from confluent_kafka import Consumer, Producer

# Supervised API Details to send Data
supervised_alert_url = "http://192.168.224.16:8080/data"

# Creating a Kafka producer instance
statistical_alert_topic = "transaction_statistical_alerts"
statistical_alert_producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "statistical_alert_producer"
})

# Creating Session for Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
processed_database = "processed"
neo4j_driver = GraphDatabase.driver(uri, auth=(username, password))
neo4j_alert_session = GraphDatabase.driver(uri, auth=(username, password)).session(database=processed_database)

def neo4j_processed(session, transaction):
    # Transaction Node
    block_transaction_query = """
    MATCH (block:Block {number: $block_number})
    MERGE (transaction:Transaction {txid: $txid})
    SET transaction.block_number = $block_number,
        transaction.total_bitcoin_transacted = $total_bitcoin_transacted
    WITH transaction, block
    MERGE (transaction)-[:INCLUDED_IN]->(block)
    """
    session.run(block_transaction_query, txid=transaction["txid"], block_number=transaction["block_number"],
                total_bitcoin_transacted=transaction["total_bitcoin_transacted"]
    )

    # VOUT Address Node
    for destination in transaction["vout"]:
        destination_transaction_id = str(transaction["txid"]) + "_" + str(destination["n"])
        if(all(op not in destination["scriptPubKey"]["asm"] for op in ("OP_RETURN", "OP_CHECKMULTISIG"))):
            destination_address = destination["scriptPubKey"]["address"]
        else:
            destination_address = None
        destination_address_transaction_query = """
        MERGE (subtransaction:SubTransaction {txid: $sub_txid})
        SET subtransaction.address = $destination_address,
            subtransaction.value = $value,
            subtransaction.is_utxo = $is_utxo,
            subtransaction.Transaction_type = $Transaction_type
        with subtransaction
        MATCH (transaction:Transaction {txid: $txid})
        MERGE (transaction)-[:OUTPUTS]->(subtransaction)
        """
        session.run(destination_address_transaction_query, sub_txid=destination_transaction_id, destination_address=destination_address,
                    value=destination["value"], is_utxo=destination["is_utxo"], Transaction_type=destination["Transaction_type"], txid=transaction["txid"]
        )

    # VIN Address Node
    statistical_vin = []
    supervised_vin = []
    for source in transaction["vin"]:
        source_address = source["address"]
        statistical_vin.append({"address": source_address, "value": source["value"], "Transaction_type": source["Transaction_type"]})
        supervised_vin.append({"txid": source["txid"], "value": source["value"], "sender_mean_sent": source["sender_mean_sent"], "sender_total_sent": source["sender_total_sent"]})
        source_address_transaction_query = """
        MERGE (subtransaction:SubTransaction {txid: $sub_txid})
        SET subtransaction.address = $source_address,
            subtransaction.value = $value,
            subtransaction.Transaction_type = $Transaction_type,
            subtransaction.supervised_alert = 0,
            subtransaction.supervised_alert_probability = 0
        with subtransaction
        MATCH (transaction:Transaction {txid: $txid})
        MERGE (subtransaction)-[:INPUTS]->(transaction)
        """
        session.run(source_address_transaction_query, sub_txid=source["txid"], source_address=source_address, value=source["value"],
                    Transaction_type=source["Transaction_type"], txid=transaction["txid"]
        )

    # Sending Data For Statistical Alert Check
    transaction_statistical_info = {
        "block_number": transaction["block_number"], "txid": transaction["txid"], "value": transaction["total_bitcoin_transacted"],
        "time": transaction["time"], "source": statistical_vin
    }
    kafka_produce_alerts(statistical_alert_producer, statistical_alert_topic, transaction_statistical_info)

    # Sending Data For Supervised Alert Check
    transaction_supervised_info = {
        "block_number": transaction["block_number"], "maintxid": transaction["txid"], "total_bitcoin_transacted": transaction["total_bitcoin_transacted"],
        "fee": transaction["fee"], "time": transaction["time"], "receiver_mean_received": transaction["receiver_mean_received"],
        "receiver_total_received": transaction["receiver_total_received"], "vin": supervised_vin
    }
    df = pd.DataFrame(transaction_supervised_info)
    df["vin"] = df["vin"].apply(lambda x: json.loads(str(x).replace("'", "\"")))
    df = pd.concat([df.drop(columns=["vin"]), pd.json_normalize(df["vin"])], axis=1)
    transaction_supervised_info = df.to_dict(orient="records")
    #api_produce_alerts(transaction_supervised_info)
    return True

def kafka_produce_alerts(kafka_producer, topic, transaction_message):
    kafka_producer.produce(topic=topic, value=json.dumps(transaction_message).encode("utf-8"))
    kafka_producer.flush()
    return True

def api_produce_alerts(transaction_message):
    post_flag = 0
    while not post_flag:
        response = requests.post(supervised_alert_url, json=json.dumps(transaction_message))
        if(response.status_code == 200):
            for res in response.json():
                alert_query = """
                Match (n:SubTransaction {txid: $txid})
                CALL apoc.lock.nodes([n])
                WITH n
                SET n.supervised_alert = 1,
                    n.supervised_alert_probability = $illegal_probability
                RETURN n
                """
                neo4j_alert_session.run(alert_query, txid=res["tx_hash_from"], illegal_probability=res["illegal_probability"])
            post_flag = 1
        else:
            print("Response Status Code:", response.status_code, "Response Content:", response.text)
            time.sleep(10)
    return True


def consume_messages(topic, config, consumer_id):
    consumer = Consumer(config)
    consumer.subscribe([topic])
    with neo4j_driver.session(database=processed_database) as session:
        while True:
            message = consumer.poll(5000)
            try:
                if (message is not None):
                    transaction = json.loads(message.value().decode("utf-8"))
                    if(any(data.get("address") == "Coinbase Transaction" for data in transaction["vin"])):
                        pass
                    else:
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
