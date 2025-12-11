# spark-processor/processor.py
"""
PySpark streaming job skeleton:
- consume from Kafka (Debezium topics)
- parse Debezium JSON envelope
- transform/clean
- upsert into Postgres (warehouse)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

def main():
    spark = SparkSession.builder \
        .appName("cdc-etl-processor") \
        .getOrCreate()

    # TODO: set the correct Kafka bootstrap server/subscribe topic
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:9092") \
        .option("subscribe", "dbserver1.public.orders") \
        .option("startingOffsets", "earliest") \
        .load()

    # value is Debezium JSON; we parse it in a later phase
    raw = df.selectExpr("CAST(value AS STRING) as value")

    # placeholder schema — update to match Debezium unwrap or envelope
    schema = StructType([
        StructField("order_id", IntegerType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("status", StringType(), True),
        StructField("created_at", TimestampType(), True),
        StructField("updated_at", TimestampType(), True)
    ])

    parsed = raw  # replace with JSON parsing in Phase 5

    # Example write: For now we just print to console (dev mode)
    query = raw.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()
