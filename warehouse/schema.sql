-- warehouse/schema.sql
CREATE TABLE IF NOT EXISTS orders_fact (
    order_id INT PRIMARY KEY,
    customer_id INT,
    product_name VARCHAR(255),
    amount DECIMAL(10,2),
    status VARCHAR(50),
    cdc_timestamp TIMESTAMP,
    processed_at TIMESTAMP
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_customer ON orders_fact(customer_id);
CREATE INDEX IF NOT EXISTS idx_processed ON orders_fact(processed_at);
