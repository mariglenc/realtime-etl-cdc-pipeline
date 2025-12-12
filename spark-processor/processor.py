# spark-processor/processor.py
import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, current_timestamp, to_timestamp, lit, when
)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType
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
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

logger.info(f"Reading from Kafka topic: {KAFKA_TOPIC}")

# Read from Kafka - start simple, just get raw messages
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load()

logger.info("Parsing Kafka messages...")

# SIMPLEST POSSIBLE APPROACH: Parse the JSON dynamically
# This will infer the schema from the JSON
from pyspark.sql.functions import expr, regexp_extract

# Convert Kafka value to string and extract the payload part
parsed_df = kafka_df.select(
    col("value").cast("string").alias("json_value"),
    col("timestamp").alias("kafka_timestamp")
)

# Extract fields using JSON path
extracted_df = parsed_df.select(
    expr("get_json_object(json_value, '$.payload.after.order_id')").cast("int").alias("order_id"),
    expr("get_json_object(json_value, '$.payload.after.customer_id')").cast("int").alias("customer_id"),
    expr("get_json_object(json_value, '$.payload.after.product_name')").alias("product_name"),
    lit(0.0).alias("amount"),  # Will fix amount later
    expr("get_json_object(json_value, '$.payload.after.status')").alias("status"),
    expr("get_json_object(json_value, '$.payload.after.created_at')").cast("long").alias("created_at_micro"),
    expr("get_json_object(json_value, '$.payload.after.updated_at')").cast("long").alias("updated_at_micro"),
    expr("get_json_object(json_value, '$.payload.ts_ms')").cast("long").alias("ts_ms"),
    expr("get_json_object(json_value, '$.payload.op')").alias("operation_type"),
    col("kafka_timestamp")
)

# Filter and transform
final_df = extracted_df \
    .filter(col("order_id").isNotNull()) \
    .filter(col("operation_type").isin("c", "u", "r")) \
    .select(
        col("order_id"),
        col("customer_id"),
        col("product_name"),
        col("amount"),
        col("status"),
        to_timestamp(col("created_at_micro") / 1000000).alias("created_at"),
        to_timestamp(col("updated_at_micro") / 1000000).alias("updated_at"),
        to_timestamp(col("ts_ms") / 1000).alias("cdc_timestamp"),
        current_timestamp().alias("processed_at"),
        col("operation_type")
    )

# JDBC URL
jdbc_url = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}"

# Write function
def write_to_postgres(batch_df, batch_id):
    try:
        row_count = batch_df.count()
        logger.info(f"Processing batch {batch_id} with {row_count} records")
        
        if row_count > 0:
            # Show sample for debugging
            sample = batch_df.first()
            logger.info(f"Sample record: order_id={sample['order_id']}, customer_id={sample['customer_id']}, product={sample['product_name']}")
            
            # Write to PostgreSQL
            batch_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", PG_TABLE) \
                .option("user", PG_USER) \
                .option("password", PG_PASSWORD) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            logger.info(f"✅ Batch {batch_id} written to warehouse successfully!")
        else:
            logger.info(f"Batch {batch_id} is empty, skipping write")
    except Exception as e:
        logger.error(f"❌ Error writing batch {batch_id}: {str(e)}")
        import traceback
        logger.error(traceback.format_exc())
        # Don't raise - log and continue
        pass

# Start streaming query
logger.info("Starting streaming query...")
query = final_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_LOCATION) \
    .trigger(processingTime="10 seconds") \
    .start()

logger.info("✅ Streaming query started. Waiting for data...")
query.awaitTermination()