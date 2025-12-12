#!/bin/bash

set -e

docker compose down -v
docker compose up -d --build

sleep 5

curl -s -X POST http://localhost:8083/connectors  -H "Content-Type: application/json" -d @debezium/postgres-connector.json
curl -s http://localhost:8083/connectors/postgres-connector/status

cd source-db
# windows
# python -m venv venv
# venv\Scripts\activate

# linux
python3 -m venv venv
source venv/bin/activate

python3 seed_data.py
