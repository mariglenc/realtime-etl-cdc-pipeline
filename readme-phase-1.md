
## Setup & Usage (Phase 1)

### 1️⃣ Clone & initialize git

git clone https://github.com/mariglenc/realtime-etl-cdc-pipeline.git
cd realtime-etl-cdc-pipeline

#### Linux / macOS

```bash
python3 -m venv venv
source venv/bin/activate
```

#### Windows (CMD)

```cmd
python -m venv venv
venv\Scripts\activate.bat
```

### 3️⃣ Install Python dependencies

```bash
pip install --upgrade pip
pip install psycopg2-binary
```

### 4️⃣ Start Docker Compose (Postgres container)

```bash
docker compose up -d
docker compose ps
```

Check that Postgres container is running (example name: `realtime-etl-cdc-pipeline-postgres-1`).

---

### 5️⃣ Verify Postgres tables

#### Connect inside container

```bash
docker exec -it realtime-etl-cdc-pipeline-postgres-1 psql -U warehouse -d warehouse
```

#### List tables

```sql
\dt
```

#### Optional: create `orders` table (if not created by `init.sql`)

```sql
CREATE TABLE IF NOT EXISTS orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT NOT NULL,
    product_name VARCHAR(255),
    amount DECIMAL(10,2),
    status VARCHAR(50),
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
```

---

### 6️⃣ Seed initial data & run simulation

Run the seeder script:

```bash
python source-db/seed_data.py
```

* **Behavior:** Inserts, updates, and deletes orders continuously to simulate CDC.
* **Stop the simulation:** Press `Ctrl+C`.

---

### 7️⃣ Verify data in Postgres

Inside container:

```bash
docker exec -it realtime-etl-cdc-pipeline-postgres-1 psql -U warehouse -d warehouse
```

Query the table:

```sql
SELECT * FROM orders ORDER BY order_id;
```
