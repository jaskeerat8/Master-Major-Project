# Importing Libraries
import json, requests
import time
import pandas as pd
from neo4j import GraphDatabase
from confluent_kafka import Consumer, Producer

# Creating a Kafka producer instance
statistical_alert_topic = "transaction_statistical_alerts"
statistical_alert_producer = Producer({
    "bootstrap.servers": "localhost:9092",
    "client.id": "statistical_alert_producer"
})

# API Details to send Data
URI = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
processed_database = "processed"
supervised_alert_url = "http://192.168.224.16:8080/data"
neo4j_alert_session = GraphDatabase.driver(URI, auth=(username, password)).session(database=processed_database)

# Creating Session for Neo4j
uri = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
processed_database = "processed"
neo4j_session = GraphDatabase.driver(uri, auth=(username, password)).session(database=processed_database)

def neo4j_processed(session, transaction):
    # VOUT Address Node
    vout_address = []
    for destination in transaction["vout"]:
        if(all(op not in destination["scriptPubKey"]["asm"] for op in ("OP_RETURN", "OP_CHECKMULTISIG"))):
            destination_address = destination["scriptPubKey"]["address"]
            vout_address.append({"address": destination_address, "value": destination["value"], "is_utxo": destination["is_utxo"], "Transaction_type": destination["Transaction_type"]})
        else:
            vout_address.append({"address": None, "value": destination["value"], "is_utxo": destination["is_utxo"], "Transaction_type": destination["Transaction_type"]})

    # Transaction Node
    block_transaction_query = """
    MERGE (transaction:Transaction {txid: $txid})
    SET transaction.block_number = $block_number,
        transaction.time = $time,
        transaction.total_bitcoin_transacted = $total_bitcoin_transacted,
        transaction.fee = $fee,
        transaction.destination = $destination
    WITH transaction
    MATCH (block:Block {number: $block_number})
    MERGE (transaction)-[:INCLUDED_IN]->(block)
    """
    session.run(block_transaction_query, txid=transaction["txid"], block_number=transaction["block_number"], time=transaction["time"],
                total_bitcoin_transacted=transaction["total_bitcoin_transacted"], fee=transaction["fee"], destination=str(vout_address)
    )

    # VIN Address Node
    for source in transaction["vin"]:
        source_address_transaction_query = """
        MERGE (subtransaction:SubTransaction {txid: $sub_txid})
        SET subtransaction.address = $address,
            subtransaction.value = $value,
            subtransaction.Transaction_type = $Transaction_type,
            subtransaction.supervised_alert = 0,
            subtransaction.supervised_alert_probability = 0
        with subtransaction
        MATCH (transaction:Transaction {txid: $txid})
        MERGE (subtransaction)-[:FOR]->(transaction)
        """
        session.run(source_address_transaction_query, sub_txid=source["txid"], address=source["address"], value=source["value"],
                    Transaction_type=source["Transaction_type"], txid=transaction["txid"]
        )

    # Sending Data For Statistical Alert Check
    transaction_statistical_info = {
        "block_number": transaction["block_number"], "txid": transaction["txid"], "value": transaction["total_bitcoin_transacted"],
        "time": transaction["time"], "destination": vout_address
    }
    kafka_produce_alerts(statistical_alert_producer, statistical_alert_topic, transaction_statistical_info)

    # Sending Data For Supervised Alert Check
    vin_list = [{"txid": vin["txid"], "value": vin["value"], "sender_mean_sent": vin["sender_mean_sent"], "sender_total_sent": vin["sender_total_sent"]} for vin in transaction["vin"]]
    transaction_supervised_info = {
        "block_number": transaction["block_number"], "maintxid": transaction["txid"], "total_bitcoin_transacted": transaction["total_bitcoin_transacted"],
        "fee": transaction["fee"], "time": transaction["time"], "receiver_mean_received": transaction["receiver_mean_received"],
        "receiver_total_received": transaction["receiver_total_received"], "vin": vin_list
    }
    df = pd.DataFrame(transaction_supervised_info)
    df["vin"] = df["vin"].apply(lambda x: json.loads(str(x).replace("'", "\"")))
    df = pd.concat([df.drop(columns=["vin"]), pd.json_normalize(df["vin"])], axis=1)
    transaction_supervised_info = df.to_dict(orient="records")
    api_produce_alerts(transaction_supervised_info)
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
                supervised_alerts(res)
            post_flag = 1
        else:
            print("Response Status Code:", response.status_code, "Response Content:", response.text)
            time.sleep(10)

def supervised_alerts(response):
    alert_query = """
    Match (n:SubTransaction {txid: $txid})
    CALL apoc.lock.nodes([n])
    WITH n
    SET n.supervised_alert = 1,
        n.supervised_alert_probability = $illegal_probability
    RETURN n
    """
    neo4j_alert_session.run(alert_query, txid=response["tx_hash_from"], illegal_probability=response["illegal_probability"])
    return True


if __name__ == "__main__":
    consumer_topic = "block_transactions"
    consumer_config = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "transaction_processed_consumer",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "max.poll.interval.ms": 1000000
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([consumer_topic])

    while True:
        message = consumer.poll(5000)
        try:
            if (message is not None):
                transaction = json.loads(message.value().decode("utf-8"))
                if(any(data.get("address") == "Coinbase Transaction" for data in transaction["vin"])):
                    pass
                else:
                    print("Transaction Received In Consumer")
                    neo4j_processed(neo4j_session, transaction)
        except json.decoder.JSONDecodeError as e:
            print(f"Waiting For Data: {e}")
