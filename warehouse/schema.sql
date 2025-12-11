-- warehouse/schema.sql
CREATE TABLE IF NOT EXISTS orders_fact (
  order_id INT PRIMARY KEY,
  customer_id INT,
  product_name VARCHAR(255),
  amount NUMERIC(10,2),
  status VARCHAR(50),
  cdc_timestamp TIMESTAMP,
  processed_at TIMESTAMP
);
