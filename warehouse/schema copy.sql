-- warehouse/schema.sql

-- Drop table if exists (for clean restarts)
DROP TABLE IF EXISTS orders_fact CASCADE;

-- Create the orders_fact table
CREATE TABLE orders_fact (
    order_id INT NOT NULL,
    customer_id INT,
    product_name VARCHAR(255),
    amount DECIMAL(10,2),
    status VARCHAR(50),
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
    cdc_timestamp TIMESTAMP,
    processed_at TIMESTAMP,
    operation_type VARCHAR(10),
    PRIMARY KEY (order_id, processed_at)
);

-- Indexes for performance
CREATE INDEX idx_customer ON orders_fact(customer_id);
CREATE INDEX idx_processed ON orders_fact(processed_at);
CREATE INDEX idx_status ON orders_fact(status);
CREATE INDEX idx_cdc_timestamp ON orders_fact(cdc_timestamp);

-- Create a view for latest records only
CREATE OR REPLACE VIEW orders_latest AS
SELECT DISTINCT ON (order_id) *
FROM orders_fact
ORDER BY order_id, processed_at DESC;