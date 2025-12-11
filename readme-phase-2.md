# 📘 **Phase 2 – Enable CDC & Ingest Events with Kafka + Debezium**

This phase starts the infrastructure (Postgres + Zookeeper + Kafka + Debezium), enables the Postgres Debezium connector, seeds data into the source database, and validates that CDC change events are flowing into Kafka.

# ✅ **1. Start Docker Infrastructure**

- docker compose up -d

This starts:

* PostgreSQL (source DB)
* Zookeeper
* Kafka broker
* Debezium Connect

Check that containers are running:

- docker ps


# ✅ **2. Register the Debezium Postgres Connector**

Run this command from the project root and check connector status:

- curl -X POST http://localhost:8083/connectors  -H "Content-Type: application/json"  -d @debezium/postgres-connector.json
- curl http://localhost:8083/connectors/postgres-connector/status

Expected status: `"RUNNING"`.


# ✅ **3. Create & Activate Python Virtual Environment**

### **Windows**

- python -m venv venv
- venv\Scripts\activate.bat
- pip install -r requirements.txt

- python source-db/seed_data.py

This inserts sample orders that Debezium will detect and push to Kafka.

# ✅ **4. Validate CDC Events in Kafka**

Open a Kafka console consumer inside the Kafka container:

docker exec -it realtime-etl-cdc-pipeline-kafka-1  kafka-console-consumer  --bootstrap-server kafka:9092  --topic dbserver1.public.orders  --from-beginning

You should see CDC messages like:

* `"op": "c"` → INSERT
* `"op": "u"` → UPDATE
* `"op": "d"` → DELETE
