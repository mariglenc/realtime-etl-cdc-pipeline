# source-db/seed_data.py
import time
import random
import psycopg2
from psycopg2.extras import RealDictCursor

# Connect to Postgres in Docker container
DSN = "dbname=warehouse user=warehouse password=pw host=localhost port=5432"

def get_conn():
    return psycopg2.connect(DSN, cursor_factory=RealDictCursor)

def seed_initial():
    conn = get_conn()
    cur = conn.cursor()
    # Create table if not exists (safe)
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders (
        order_id SERIAL PRIMARY KEY,
        customer_id INT NOT NULL,
        product_name VARCHAR(255),
        amount DECIMAL(10,2),
        status VARCHAR(50),
        created_at TIMESTAMP DEFAULT now(),
        updated_at TIMESTAMP DEFAULT now()
    );
    """)
    conn.commit()

    # Seed some initial rows
    for i in range(1, 6):
        cur.execute("""
        INSERT INTO orders (customer_id, product_name, amount, status)
        VALUES (%s, %s, %s, %s)
        RETURNING order_id;
        """, (i, f"product_{i}", round(random.uniform(10, 200), 2), "NEW"))
        order_id = cur.fetchone()['order_id']
        print("Seeded order:", order_id)
    conn.commit()
    cur.close()
    conn.close()

def run_simulation():
    conn = get_conn()
    cur = conn.cursor()
    while True:
        op = random.choices(["insert", "update", "delete"], weights=[0.6, 0.35, 0.05])[0]

        if op == "insert":
            cur.execute("""
            INSERT INTO orders (customer_id, product_name, amount, status)
            VALUES (%s, %s, %s, %s)
            RETURNING order_id;
            """, (random.randint(1, 10), f"product_{random.randint(1,20)}", round(random.uniform(5,300),2), "NEW"))
            order_id = cur.fetchone()['order_id']
            print("Inserted order:", order_id)

        elif op == "update":
            cur.execute("SELECT order_id FROM orders ORDER BY random() LIMIT 1;")
            row = cur.fetchone()
            if row:
                cur.execute("UPDATE orders SET amount = %s, status = %s, updated_at = now() WHERE order_id = %s",
                            (round(random.uniform(5,300),2), random.choice(["NEW","PROCESSING","SHIPPED"]), row['order_id']))
                print("Updated order:", row['order_id'])

        elif op == "delete":
            cur.execute("SELECT order_id FROM orders ORDER BY random() LIMIT 1;")
            row = cur.fetchone()
            if row:
                cur.execute("DELETE FROM orders WHERE order_id = %s", (row['order_id'],))
                print("Deleted order:", row['order_id'])

        conn.commit()
        time.sleep(random.uniform(1.0, 3.0))

if __name__ == "__main__":
    seed_initial()
    print("Starting simulation. Orders table will be updated continuously.")
    run_simulation()
