import psycopg2
import random
import time
from datetime import datetime

conn = psycopg2.connect(
    dbname="orders_db",
    user="postgres",
    password="password",
    host="127.0.0.1",
    port=5432
)

cur = conn.cursor()

products = ["Widget A", "Widget B", "Gadget X", "Gadget Y"]
statuses = ["pending", "shipped", "delivered"]

while True:
    op = random.choice(["insert", "insert", "insert", "insert", "update", "delete"])
    # op = random.choice(["insert", "update", "delete"])

    if op == "insert":
        cur.execute(
            "INSERT INTO orders (customer_id, product_name, amount, status) VALUES (%s, %s, %s, %s) RETURNING order_id",
            (random.randint(1, 1000), random.choice(products), round(random.uniform(10, 500), 2), random.choice(statuses))
        )
        order_id = cur.fetchone()[0]
        print(f"Inserted order {order_id}")

    elif op == "update":
        cur.execute("SELECT order_id FROM orders ORDER BY RANDOM() LIMIT 1")
        row = cur.fetchone()
        if row:
            order_id = row[0]
            cur.execute(
                "UPDATE orders SET status = %s, updated_at = %s WHERE order_id = %s",
                (random.choice(statuses), datetime.now(), order_id)
            )
            print(f"Updated order {order_id}")

    elif op == "delete":
        cur.execute("SELECT order_id FROM orders ORDER BY RANDOM() LIMIT 1")
        row = cur.fetchone()
        if row:
            order_id = row[0]
            cur.execute("DELETE FROM orders WHERE order_id = %s", (order_id,))
            print(f"Deleted order {order_id}")

    conn.commit()
    time.sleep(1)
