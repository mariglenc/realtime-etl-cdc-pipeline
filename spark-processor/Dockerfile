# spark-processor/Dockerfile
FROM spark:3.4.1

USER root

WORKDIR /app

# Install Python and pip
RUN apt-get update && \
    apt-get install -y python3-pip curl procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Copy requirements and install dependencies
COPY requirements.txt /app/requirements.txt
RUN pip3 install --no-cache-dir -r requirements.txt

# Create necessary directories
RUN mkdir -p /tmp/spark_checkpoints /tmp/spark_dlq && \
    chmod -R 777 /tmp/spark_checkpoints /tmp/spark_dlq

# Copy application code
COPY processor.py /app/processor.py

# Set PYSPARK_PYTHON to use python3
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Submit Spark job with packages downloaded at runtime
CMD ["spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.postgresql:postgresql:42.7.0", \
     "--conf", "spark.sql.streaming.checkpointLocation=/tmp/spark_checkpoints", \
     "/app/processor.py"]