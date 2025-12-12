-- ===============================
-- Analytics Queries for orders_fact
-- ===============================

-- 1️⃣ Last 10 processed orders
SELECT *
FROM orders_fact
ORDER BY processed_at DESC
LIMIT 10;

-- 2️⃣ Total number of orders per customer
SELECT customer_id,
       COUNT(*) AS total_orders,
       SUM(amount) AS total_amount
FROM orders_fact
GROUP BY customer_id
ORDER BY total_orders DESC
LIMIT 20;

-- 3️⃣ Orders by status
SELECT status,
       COUNT(*) AS count,
       SUM(amount) AS total_amount
FROM orders_fact
GROUP BY status
ORDER BY count DESC;

-- 4️⃣ Daily order volume
SELECT DATE(processed_at) AS day,
       COUNT(*) AS orders_count,
       SUM(amount) AS total_amount
FROM orders_fact
GROUP BY DATE(processed_at)
ORDER BY day DESC
LIMIT 30;

-- 5️⃣ Top products by revenue
SELECT product_name,
       COUNT(*) AS orders_count,
       SUM(amount) AS total_amount
FROM orders_fact
GROUP BY product_name
ORDER BY total_amount DESC
LIMIT 10;

-- 6️⃣ Recently updated orders (last 24 hours)
SELECT *
FROM orders_fact
WHERE processed_at >= NOW() - INTERVAL '1 day'
ORDER BY processed_at DESC;

-- 7️⃣ Average order amount per customer
SELECT customer_id,
       AVG(amount) AS avg_order_amount
FROM orders_fact
GROUP BY customer_id
ORDER BY avg_order_amount DESC
LIMIT 20;
