![Capstone thesis](https://github.com/jaskeerat8/Master-Major-Project/assets/32131898/726f8459-cdab-41f7-bdb6-727023be2c41)
Using Kafka and Neo4j to process real-time incoming Bitcoin data. The system uses a front-end visualization developed by another member to show the results. The thesis report and seminar presentation are present in the University-of-Queensland repository.

* The system consists of a source file used to connect to the Bitcoin RPC server. This Python file retrieves the data and passes it to the Kafka Producer.
* The system uses 2 Kafka Consumer Groups to pass the data along to 2 different processes - a clean method to store every data point and a pruned method to store important data points.
* In the clean process, a Neo4j schema is defined to store data points interconnected with other incoming data.
* In the pruned process, a Neo4j schema is defined to store data points which is required to build and drive advanced visualizations.
* In a separate process, the anomaly detection method of DBSCAN Clustering is used to analyse the incoming data points and separate the anomalous data points. The results are then updated and stored in the Neo4j graph database.

The data stored in Neo4j is now available to the end user via a REST API which consists of 3 different routes to serve the requirements of the visualization.
