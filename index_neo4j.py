# Importing Libraries
from neo4j import GraphDatabase

# Creating Session for Neo4j
URI = "bolt://localhost:7687"
username = "neo4j"
password = "capstone"
neo4j_driver = GraphDatabase.driver(URI, auth=(username, password))

list_of_queries = ["CREATE INDEX FOR (b:Block) ON (b.number);", "CREATE INDEX FOR (t:Transaction) ON (t.id);", "CREATE INDEX FOR (a:Address) ON (a.address);"]

for db in ["clean", "processed"]:
    for index_query in list_of_queries:
        with neo4j_driver.session(database=db) as session:
            try:
                session.run(index_query)
                print("Index Created")
            except Exception as e:
                print(f"Fault in Creation: {e}")
