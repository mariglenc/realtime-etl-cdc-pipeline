# spark-processor\processor.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, TimestampType
import psycopg2

# 1. Create Spark session with Kafka and Postgres dependencies
spark = SparkSession.builder \
    .appName("CDC-ETL") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0,"
            "org.postgresql:postgresql:42.6.0") \
    .getOrCreate()

# 2. Define Debezium CDC schema
schema = StructType([
    StructField("before", StructType([
        StructField("order_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("product_name", StringType()),
        StructField("amount", DecimalType(10, 2)),
        StructField("status", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType())
    ])),
    StructField("after", StructType([
        StructField("order_id", IntegerType()),
        StructField("customer_id", IntegerType()),
        StructField("product_name", StringType()),
        StructField("amount", DecimalType(10, 2)),
        StructField("status", StringType()),
        StructField("created_at", TimestampType()),
        StructField("updated_at", TimestampType())
    ])),
    StructField("op", StringType()),  # c/u/d
    StructField("ts_ms", StringType())
])

# 3. Read from Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "dbserver1.public.orders") \
    .option("startingOffsets", "earliest") \
    .load()

# 4. Parse JSON payload
parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")) \
              .select("data.*")

# 5. Transform: take 'after' for inserts/updates, handle deletes separately
transformed_df = parsed_df \
    .withColumn("order_id", col("after.order_id")) \
    .withColumn("customer_id", col("after.customer_id")) \
    .withColumn("product_name", col("after.product_name")) \
    .withColumn("amount", col("after.amount")) \
    .withColumn("status", col("after.status")) \
    .withColumn("created_at", col("after.created_at")) \
    .withColumn("updated_at", col("after.updated_at")) \
    .withColumn("cdc_timestamp", col("ts_ms").cast(TimestampType())) \
    .withColumn("processed_at", current_timestamp()) \
    .filter(col("op").isin("c", "u"))  # ignore deletes

# 6. Define function to write each micro-batch to Postgres
def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .mode("append") \
        .option("url", "jdbc:postgresql://warehouse:5432/analytics") \
        .option("dbtable", "orders_fact") \
        .option("user", "postgres") \
        .option("password", "password") \
        .save()

# 7. Start streaming query with foreachBatch
query = transformed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start()

query.awaitTermination()
