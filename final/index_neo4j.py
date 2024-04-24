# Importing Libraries
from neo4j import GraphDatabase

# Creating Session for Neo4j
URI = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))

list_of_queries = {
    "clean": ["CREATE INDEX FOR (b:Block) ON (b.number);", "CREATE INDEX FOR (t:Transaction) ON (t.txid);"],
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
