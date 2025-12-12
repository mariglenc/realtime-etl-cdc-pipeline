-- warehouse/schema.sql (SUPER SIMPLE VERSION)

-- Drop existing table
DROP TABLE IF EXISTS orders_fact CASCADE;
DROP VIEW IF EXISTS orders_latest CASCADE;
DROP VIEW IF EXISTS sales_dashboard CASCADE;

-- Main table: stores ALL versions of each order
CREATE TABLE orders_fact (
    id SERIAL PRIMARY KEY,
    order_id INT NOT NULL,
    customer_id INT,
    product_name VARCHAR(255),
    amount DECIMAL(10,2),
    status VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    cdc_timestamp TIMESTAMP,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    operation_type VARCHAR(10)
);

-- Index for fast lookups
CREATE INDEX idx_order_id ON orders_fact(order_id);
CREATE INDEX idx_customer_id ON orders_fact(customer_id);

-- View: shows only the LATEST version of each order
CREATE VIEW orders_latest AS
SELECT DISTINCT ON (order_id) 
    order_id,
    customer_id,
    product_name,
    amount,
    status,
    created_at,
    updated_at,
    processed_at
FROM orders_fact
ORDER BY order_id, processed_at DESC;

-- View: Real-time sales dashboard
CREATE VIEW sales_dashboard AS
SELECT 
    status,
    COUNT(*) as order_count,
    SUM(amount) as total_revenue,
    AVG(amount) as avg_order_value,
    MAX(processed_at) as last_updated
FROM orders_latest
GROUP BY status;