-- star_schema_design.sql
-- Phase 2, Lesson 3: Star vs Snowflake
-- Goal: Design a high-performance Star Schema.

-- 🏗️ Phase 1: Absolute Foundations (Beginner)
-- The Fact Table (The Verbs/Actions)
CREATE TABLE fact_sales (
    sale_id SERIAL PRIMARY KEY,
    date_key INT,
    product_key INT,
    store_key INT,
    amount DECIMAL(15,2),
    quantity INT
);

-- 🚀 Phase 2: Intermediate (Developer)
-- The Dimension Tables (The Nouns/Context)
CREATE TABLE dim_product (
    product_key INT PRIMARY KEY,
    product_name TEXT,
    category TEXT,
    brand TEXT
);

CREATE TABLE dim_store (
    store_key INT PRIMARY KEY,
    store_name TEXT,
    city TEXT,
    region TEXT
);

-- 🏛️ Phase 3: Architect (Professional)
-- The Star join (Fastest Join Type)
-- SELECT 
--    s.region, 
--    p.category, 
--    SUM(f.amount) as total_revenue
-- FROM fact_sales f
-- JOIN dim_product p ON f.product_key = p.product_key
-- JOIN dim_store s ON f.store_key = s.store_key
-- GROUP BY 1, 2;

-- 🏛️ Architect's Tip:
-- "In a Star Schema, keep your Fact table 'Skinny and Tall' (many rows, 
-- few columns) and your Dimensions 'Fat and Short' (few rows, many columns). 
-- This structure is optimized for the 'Join' logic of modern SQL engines."
