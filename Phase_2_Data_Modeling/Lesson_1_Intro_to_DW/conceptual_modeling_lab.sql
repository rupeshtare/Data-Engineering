-- conceptual_modeling_lab.sql
-- Phase 2, Lesson 1: Introduction to Data Warehousing
-- Goal: Understand the difference between OLTP and OLAP structures.

-- 🏗️ Phase 1: Absolute Foundations (Beginner)
-- Creating a simple transaction table (OLTP style)
CREATE TABLE raw_sales (
    transaction_id INT,
    customer_name TEXT,
    product_name TEXT,
    amount DECIMAL(10,2),
    sale_date TIMESTAMP
);

-- 🚀 Phase 2: Intermediate (Developer)
-- Extracting data into a staging area for the Data Warehouse
CREATE VIEW stg_sales AS
SELECT 
    transaction_id,
    UPPER(customer_name) as customer_name, -- Cleaning data
    product_name,
    amount,
    CAST(sale_date AS DATE) as sale_date
FROM raw_sales;

-- 🏛️ Phase 3: Architect (Professional)
-- Thinking about partitioning and storage
-- In a real DW like Snowflake or BigQuery, we would define clustering keys here.
-- SELECT * FROM stg_sales WHERE sale_date = '2024-01-01';

-- 🏛️ Architect's Tip:
-- "An OLTP system is for 'Right Now' (Who just bought a coffee?). 
-- An OLAP system (Data Warehouse) is for 'Everything' (How much coffee 
-- did we sell in Seattle over the last 3 years?). Never run heavy 
-- analytical queries on your production OLTP database!"
