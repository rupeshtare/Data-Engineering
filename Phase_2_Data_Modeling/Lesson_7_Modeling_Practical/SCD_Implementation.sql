-- SCD_Implementation.sql
-- Beginner Level to Architect Level

-- 1. BEGINNER: Setup the table
CREATE TABLE IF NOT EXISTS dim_products_history (
    product_sk SERIAL PRIMARY KEY, -- The "Architect's key" (Surrogate Key)
    product_id INT,
    product_name STRING,
    price DECIMAL(10,2),
    is_active BOOLEAN,
    created_at TIMESTAMP,
    expired_at TIMESTAMP
);

-- 2. INTERMEDIATE: Initial Insert
INSERT INTO dim_products_history (product_id, product_name, price, is_active, created_at)
VALUES (101, 'Data Engineering Book', 29.99, TRUE, CURRENT_TIMESTAMP());

-- 3. ARCHITECT: The SCD Type 2 Update
-- Problem: Price changes from 29.99 to 39.99. We want to keep history.

-- Step A: Expire the old record
UPDATE dim_products_history 
SET is_active = FALSE, expired_at = CURRENT_TIMESTAMP() 
WHERE product_id = 101 AND is_active = TRUE;

-- Step B: Insert the new price record
INSERT INTO dim_products_history (product_id, product_name, price, is_active, created_at)
VALUES (101, 'Data Engineering Book', 39.99, TRUE, CURRENT_TIMESTAMP());

-- 🏛️ Architect's Tip:
-- "Notice the use of 'product_sk' (Surrogate Key). In a warehouse, 
-- NEVER use the primary key from the source app. Always generate 
-- your own keys so you can track multiple versions of the same product."
