# source-db/seed_data.py
import time
import random
import psycopg2
from psycopg2.extras import RealDictCursor

DSN = "dbname=warehouse user=warehouse password=pw host=localhost port=5432"

def get_conn():
    return psycopg2.connect(DSN, cursor_factory=RealDictCursor)

def seed_initial():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM orders;")
    try:
        count = cur.fetchone()['count']
    except:
        count = None
    # Try creating table if not exists
    cur.execute(open("source-db/init.sql").read())
    conn.commit()

    # seed some rows
    for i in range(1,6):
        cur.execute("""
            INSERT INTO orders (customer_id, product_name, amount)
            VALUES (%s, %s, %s)
            ON CONFLICT DO NOTHING;
        """, (i, f"product_{i}", round(random.uniform(10,200),2)))
    conn.commit()
    cur.close()
    conn.close()

def run_simulation():
    conn = get_conn()
    cur = conn.cursor()
    while True:
        # random insert or update or delete with low probability
        op = random.choices(["insert","update","delete"], weights=[0.6,0.35,0.05])[0]
        if op == "insert":
            cur.execute("""
                INSERT INTO orders (customer_id, product_name, amount)
                VALUES (%s,%s,%s) RETURNING order_id;
            """, (random.randint(1,10), f"product_{random.randint(1,20)}", round(random.uniform(5,300),2)))
            print("inserted", cur.fetchone()[0])
        elif op == "update":
            cur.execute("SELECT order_id FROM orders ORDER BY random() LIMIT 1;")
            row = cur.fetchone()
            if row:
                cur.execute("UPDATE orders SET amount = %s, updated_at = now() WHERE order_id = %s",
                            (round(random.uniform(5,300),2), row[0]))
                print("updated", row[0])
        elif op == "delete":
            cur.execute("SELECT order_id FROM orders ORDER BY random() LIMIT 1;")
            row = cur.fetchone()
            if row:
                cur.execute("DELETE FROM orders WHERE order_id = %s", (row[0],))
                print("deleted", row[0])
        conn.commit()
        time.sleep(random.uniform(1.0, 4.0))

if __name__ == "__main__":
    seed_initial()
    print("Starting simulation. Connect to Postgres and watch the orders table.")
    run_simulation()
