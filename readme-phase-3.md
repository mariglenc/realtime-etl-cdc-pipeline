# ✅ **PHASE 3 — Spark Structured Streaming Processor**

Process real-time CDC events from Kafka and write clean analytics data into the warehouse.

---

# 🎯 **Goal of Phase 3**

In this phase you will:

* Consume **Debezium CDC events** from Kafka
* Parse the Debezium envelope (`before`, `after`, `op`, etc.)
* Transform the data into a clean analytics model
* Write streaming output into the warehouse Postgres (demo)
* Keep end-to-end CDC pipeline running continuously

---

# 📂 **Directory: `spark-processor/`**

```
spark-processor/
│── processor.py
│── requirements.txt
│── Dockerfile
```

---

# ⚡ **1. Install dependencies**

### **Windows**

```bash
cd spark-processor
python -m venv venv
venv\Scripts\activate

pip install -r requirements.txt
```
---

# ⚙️ **2. Start Spark (local mode)**

You can run Spark **without Docker** since this is a lightweight job.

Make sure Kafka from Phase 2 is still running:

```bash
docker ps
```

Required containers:

* Zookeeper
* Kafka
* Kafka Connect
* Source Postgres
* (Optional) Warehouse Postgres

---

# 🧠 **3. Run the Spark streaming job**

### **Windows**

```bash
venv\Scripts\activate
cd spark-processor

python processor.py
```


---

# 🔄 **4. What the Spark job does**

### **1. Reads the Kafka topic**

```
dbserver1.public.orders
```

### **2. Parses the Debezium CDC envelope**

Example Debezium message:

```json
{
  "before": {...},
  "after": {...},
  "op": "c",
  "ts_ms": 1710000000000
}
```

### **3. Applies transformations**

* Converts `op` codes → readable actions

  * `"c"` → INSERT
  * `"u"` → UPDATE
  * `"d"` → DELETE
* Extracts `after.*` fields
* Adds `processed_at` timestamp

### **4. Writes cleaned records to the warehouse**

Destination table:

```
warehouse.public.orders_fact
```

---

# 🧪 **5. Validate the output in warehouse**

Connect to the warehouse Postgres:

```bash
docker exec -it realtime-etl-cdc-pipeline-postgres-warehouse psql -U warehouse -d analytics
```

Then:

```sql
SELECT * FROM orders_fact ORDER BY processed_at DESC LIMIT 10;
```

If records appear → **Spark is working.**

---

# 📌 **6. Useful monitoring commands**

### **See raw Kafka CDC events**

```bash
docker exec -it realtime-etl-cdc-pipeline-kafka-1 kafka-console-consumer \
  --bootstrap-server kafka:9092 \
  --topic dbserver1.public.orders \
  --from-beginning
```

### **Spark UI**

Open in browser:

```
http://localhost:4040
```

Shows:

* Streaming jobs
* Throughput
* Batch duration
* Failures

---

# 🧹 **7. Stop the processor**

Press:

```
CTRL + C
```

---

# 🎉 Phase 3 Complete!

At this point you now have:

* PostgreSQL generating live data
* Debezium capturing CDC
* Kafka streaming events
* Spark transforming CDC → analytics
* Warehouse receiving final fact table

---

# Ready for Phase 4?

I can create:

✅ Phase 4 — Warehouse Schema + Analytics Queries
OR
✅ Phase 4 — Dockerizing Spark Job
OR
✅ Phase 4 — Improving latency, checkpoints, fault-tolerance

Which one should I generate next?
