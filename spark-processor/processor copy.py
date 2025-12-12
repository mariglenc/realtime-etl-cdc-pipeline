# spark-processor/processor.py
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, to_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    DecimalType, TimestampType, LongType
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "dbserver1.public.orders")
PG_HOST = os.getenv("PG_HOST", "postgres-warehouse")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "analytics")
PG_USER = os.getenv("PG_USER", "warehouse")
PG_PASSWORD = os.getenv("PG_PASSWORD", "password")
PG_TABLE = os.getenv("PG_TABLE", "orders_fact")
CHECKPOINT_LOCATION = os.getenv("CHECKPOINT_LOCATION", "/tmp/spark_checkpoints/orders")

# Create Spark session
logger.info("Initializing Spark Session...")
spark = SparkSession.builder \
    .appName("CDC-ETL-Pipeline") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Define nested schema for Debezium CDC payload
payload_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("amount", DecimalType(10, 2), True),
    StructField("status", StringType(), True),
    StructField("created_at", LongType(), True),  # Debezium sends timestamps as microseconds
    StructField("updated_at", LongType(), True)
])

# Complete Debezium envelope schema
debezium_schema = StructType([
    StructField("before", payload_schema, True),
    StructField("after", payload_schema, True),
    StructField("op", StringType(), True),
    StructField("ts_ms", LongType(), True),  # Timestamp in milliseconds
    StructField("source", StructType([
        StructField("version", StringType(), True),
        StructField("connector", StringType(), True),
        StructField("name", StringType(), True),
        StructField("ts_ms", LongType(), True),
        StructField("snapshot", StringType(), True),
        StructField("db", StringType(), True),
        StructField("schema", StringType(), True),
        StructField("table", StringType(), True)
    ]), True)
])

logger.info(f"Reading from Kafka topic: {KAFKA_TOPIC}")

# Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

logger.info("Parsing Kafka messages...")

# Parse JSON payload with error handling
parsed_df = df.select(
    col("key").cast("string").alias("message_key"),
    from_json(col("value").cast("string"), debezium_schema).alias("data"),
    col("timestamp").alias("kafka_timestamp")
).select("message_key", "data.*", "kafka_timestamp")

# Transform: Extract fields from 'after' for inserts/updates
transformed_df = parsed_df \
    .filter(col("op").isin("c", "u", "r")) \
    .select(
        col("after.order_id").alias("order_id"),
        col("after.customer_id").alias("customer_id"),
        col("after.product_name").alias("product_name"),
        col("after.amount").alias("amount"),
        col("after.status").alias("status"),
        to_timestamp(col("after.created_at") / 1000000).alias("created_at"),
        to_timestamp(col("after.updated_at") / 1000000).alias("updated_at"),
        to_timestamp(col("ts_ms") / 1000).alias("cdc_timestamp"),
        current_timestamp().alias("processed_at"),
        col("op").alias("operation_type")
    ) \
    .filter(col("order_id").isNotNull())

# JDBC URL
jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# Write function for each micro-batch
def write_to_postgres(batch_df, batch_id):
    try:
        logger.info(f"Processing batch {batch_id} with {batch_df.count()} records")
        
        # Write to PostgreSQL with upsert logic
        batch_df.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", PG_TABLE) \
            .option("user", PG_USER) \
            .option("password", PG_PASSWORD) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        
        logger.info(f"Batch {batch_id} written successfully")
    except Exception as e:
        logger.error(f"Error writing batch {batch_id}: {str(e)}")
        raise

# Start streaming query
logger.info("Starting streaming query...")
query = transformed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .trigger(processingTime="10 seconds") \
    .start()

logger.info("Streaming query started. Waiting for termination...")
query.awaitTermination()