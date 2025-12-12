# spark-processor\Dockerfile
FROM spark:3.4.1

WORKDIR /app

COPY requirements.txt /app/requirements.txt

USER root

RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install --no-cache-dir -r requirements.txt && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /tmp/spark_checkpoints /tmp/spark_dlq

COPY . /app

ENTRYPOINT ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1", "processor.py"]
