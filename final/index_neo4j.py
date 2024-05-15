# Importing Libraries
import yaml
from neo4j import GraphDatabase

# Reading Configurations
with open("configurations.yaml") as f:
    configurations = yaml.safe_load(f)

# Creating Session for Neo4j
URI = configurations["neo4j"]["uri"]
username = configurations["neo4j"]["username"]
password = configurations["neo4j"]["password"]
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))

list_of_queries = {
    "clean-compact": ["CREATE INDEX FOR (b:Block) ON (b.number);", "CREATE INDEX FOR (t:Transaction) ON (t.txid);"],
    "processed": ["CREATE INDEX FOR (b:Block) ON (b.number);", "CREATE INDEX FOR (t:Transaction) ON (t.txid);", "CREATE INDEX FOR (s:SubTransaction) ON (s.txid);"]
}

for database, queries in list_of_queries.items():
    for query in queries:
        with neo4j_driver.session(database=database) as session:
            try:
                session.run(query)
                print("Index Created")
            except Exception as e:
                print(f"Fault in Creation: {e}")
