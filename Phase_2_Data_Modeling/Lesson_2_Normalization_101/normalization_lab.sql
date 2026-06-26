-- normalization_lab.sql
-- Phase 2, Lesson 2: Normalization 101
-- Goal: Break down a "flat" table into 3NF (Third Normal Form).

-- 🏗️ Phase 1: Absolute Foundations (Beginner)
-- The "Bad" Flat Table (Redundancy galore!)
CREATE TABLE flat_orders (
    order_id INT,
    customer_name TEXT,
    customer_email TEXT,
    product_name TEXT,
    product_price DECIMAL(10,2)
);

-- 🚀 Phase 2: Intermediate (Developer)
-- Normalized into 3NF
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    name TEXT,
    email TEXT UNIQUE
);

CREATE TABLE products (
    product_id SERIAL PRIMARY KEY,
    name TEXT,
    price DECIMAL(10,2)
);

CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INT REFERENCES customers(customer_id),
    product_id INT REFERENCES products(product_id),
    order_date DATE
);

-- 🏛️ Phase 3: Architect (Professional)
-- Why normalize? To prevent "Anomalies". 
-- If a customer changes their email, we only update it in ONE place (customers table).
-- In the flat table, we'd have to update every single order row.

-- 🏛️ Architect's Tip:
-- "Normalization is for TRANSACTIONAL systems to keep data clean. 
-- For DRAWHOUSING, we often 'De-normalize' back into flat tables 
-- to make queries faster. It's a trade-off: Cleanliness vs. Speed."
