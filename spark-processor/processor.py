from pyspark.sql import SparkSession
from pyspark.sql.functions import col, get_json_object
import os
import psycopg2
from psycopg2.extras import execute_values
import json
import time
from dotenv import load_dotenv

load_dotenv()

# Kafka and Postgres config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dbserver1.public.orders")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark_checkpoints/orders")
DLQ_PATH = os.getenv("DLQ_PATH", "/tmp/spark_dlq/malformed_messages.log")

PG_HOST = os.getenv("PG_HOST", "warehouse")
PG_PORT = os.getenv("PG_PORT", 5432)
PG_DB = os.getenv("PG_DB", "analytics")
PG_USER = os.getenv("PG_USER", "postgres")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_TABLE = os.getenv("PG_TABLE", "orders_fact")


def safe_int(val):
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def safe_float(val):
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def foreach_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    # Ensure DLQ directory exists
    os.makedirs(os.path.dirname(DLQ_PATH), exist_ok=True)

    rows = batch_df.select("op", "order_id", "customer_id", "product_name", "amount", "status", "ts_ms").collect()
    conn = None
    try:
        conn = psycopg2.connect(dbname=PG_DB, user=PG_USER, password=PG_PASSWORD, host=PG_HOST, port=PG_PORT)
        cur = conn.cursor()
        upsert_sql = f"""
            INSERT INTO {PG_TABLE} (order_id, customer_id, product_name, amount, status, cdc_timestamp, processed_at)
            VALUES %s
            ON CONFLICT (order_id)
            DO UPDATE SET
                customer_id = EXCLUDED.customer_id,
                product_name = EXCLUDED.product_name,
                amount = EXCLUDED.amount,
                status = EXCLUDED.status,
                cdc_timestamp = EXCLUDED.cdc_timestamp,
                processed_at = EXCLUDED.processed_at;
        """
        upsert_values = []
        delete_ids = []

        for r in rows:
            ts_ms = safe_int(r['ts_ms'])

            if r['op'] in ("c", "u"):
                amount_val = safe_float(r['amount'])
                if amount_val is None and r['amount'] is not None:
                    # Malformed number, send to DLQ
                    with open(DLQ_PATH, "a", encoding="utf-8") as f:
                        f.write(json.dumps(r) + "\n")
                    continue

                upsert_values.append((
                    safe_int(r['order_id']),
                    safe_int(r['customer_id']),
                    r['product_name'],
                    amount_val,
                    r['status'],
                    time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime(ts_ms / 1000)) if ts_ms else None,
                    time.strftime('%Y-%m-%d %H:%M:%S', time.gmtime())
                ))
            elif r['op'] == "d":
                delete_ids.append(safe_int(r['order_id']))
            else:
                with open(DLQ_PATH, "a", encoding="utf-8") as f:
                    f.write(json.dumps(r) + "\n")

        if delete_ids:
            cur.execute(f"DELETE FROM {PG_TABLE} WHERE order_id = ANY(%s)", (delete_ids,))
        if upsert_values:
            execute_values(cur, upsert_sql, upsert_values)
        conn.commit()
    except Exception as e:
        with open(DLQ_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps({"error": str(e), "batch_id": batch_id}) + "\n")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()


def main():
    spark = SparkSession.builder \
        .appName("cdc-etl-processor") \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    # Extract key Debezium fields
    raw_df = df.selectExpr("CAST(value AS STRING) AS value_str")
    parsed_df = raw_df.withColumn("op", get_json_object(col("value_str"), "$.payload.op")) \
                      .withColumn("order_id", get_json_object(col("value_str"), "$.payload.after.order_id")) \
                      .withColumn("customer_id", get_json_object(col("value_str"), "$.payload.after.customer_id")) \
                      .withColumn("product_name", get_json_object(col("value_str"), "$.payload.after.product_name")) \
                      .withColumn("amount", get_json_object(col("value_str"), "$.payload.after.amount")) \
                      .withColumn("status", get_json_object(col("value_str"), "$.payload.after.status")) \
                      .withColumn("ts_ms", get_json_object(col("value_str"), "$.payload.ts_ms"))

    query = parsed_df.writeStream \
        .foreachBatch(foreach_batch) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .start()

    query.awaitTermination()


if __name__ == "__main__":
    main()
