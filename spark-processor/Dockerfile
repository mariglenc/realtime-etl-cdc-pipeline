FROM openjdk:11-jre-slim

# You may replace with a Spark base image in Phase 5
WORKDIR /app
COPY . /app

# Install python and pip
RUN apt-get update && apt-get install -y python3 python3-pip && \
    pip3 install --no-cache-dir -r requirements.txt

ENTRYPOINT ["python3", "processor.py"]
