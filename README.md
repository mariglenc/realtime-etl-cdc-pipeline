# realtime-etl-cdc-pipeline

Minimal starter for the "Real-Time ETL Pipeline with CDC" challenge.

## Overview
This repo contains a Phase-1 starter:
- Project skeleton
- Docker Compose placeholder (infra will be added)
- Source DB init and simple data seeder
- Debezium connector JSON (placeholder)
- Spark processor skeleton
- Warehouse schema

Follow the README phases to expand into a full pipeline:
1. Configure Postgres for logical replication
2. Run Kafka + Debezium (via Docker Compose)
3. Run Spark processor
4. Validate writes into the warehouse (Postgres/Redshift)

docker compose down -v
docker compose up --build -d

curl -X POST http://localhost:8083/connectors  -H "Content-Type: application/json"  -d @debezium/postgres-connector.json
curl http://localhost:8083/connectors/postgres-connector/status

inside kafka docker shell 
kafka-topics --bootstrap-server kafka:9092 --list


1-
sh-4.4$ kafka-topics --bootstrap-server kafka:9092 --list

__consumer_offsets
connect-configs
connect-offsets
connect-status
dbserver1.public.orders
sh-4.4$ 
sh-4.4$ 
2-
(venv) C:\Users\marig\techinsight21\realtime-etl-cdc-pipeline\source-db>curl localhost:8083/connectors
["postgres-connector"]
(venv) C:\Users\marig\techinsight21\realtime-etl-cdc-pipeline\source-db>