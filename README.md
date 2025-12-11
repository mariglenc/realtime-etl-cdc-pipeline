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

## How to use (Phase 1)
1. Initialize git:
   ```bash
   git init
   git add .
   git commit -m "Initial project skeleton"
