-- delta_beginner_lab.sql
-- Beginner Level: Table creation and basic CRUD in Delta Lake.

-- 1. BEGINNER: Create your first Delta table
CREATE TABLE IF NOT EXISTS silver_orders (
    order_id INT,
    product_name STRING,
    amount DOUBLE,
    order_date DATE
) USING DELTA;

-- 2. BEGINNER: Basic Insert
INSERT INTO silver_orders VALUES (1, 'Laptop', 1200.00, '2024-03-19');

-- 3. INTERMEDIATE: Time Travel
-- Let's "Mistakenly" update data
UPDATE silver_orders SET amount = 0 WHERE order_id = 1;

-- Now, let's see the history
DESCRIBE HISTORY silver_orders;

-- Return to the original amount using Time Travel
-- (Assuming version 0 was the correct one)
SELECT * FROM silver_orders VERSION AS OF 0;

-- 🏛️ Architect's Tip:
-- "Always use 'IF NOT EXISTS' in your DDL scripts. 
-- This makes your scripts 'Idempotent', meaning they can be run 
-- multiple times without error."
