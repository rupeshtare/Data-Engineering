-- date_dimension_generator.sql
-- Phase 2, Lesson 4: The Date Dimension
-- Goal: Generate a robust Date Dimension (The "Spine" of every DW).

-- 🏗️ Phase 1: Absolute Foundations (Beginner)
-- Why not just use a TIMESTAMP?
-- Answer: Because finding "Third Wednesday of the Month" is hard with timestamps, 
-- but easy with a Date Dimension.

CREATE TABLE dim_date (
    date_key INT PRIMARY KEY, -- e.g., 20240101
    full_date DATE,
    day_of_week TEXT,
    month_name TEXT,
    quarter INT,
    year INT,
    is_weekend BOOLEAN
);

-- 🚀 Phase 2: Intermediate (Developer)
-- A simple SQL generator (PostgreSQL style)
-- INSERT INTO dim_date
-- SELECT
--     TO_CHAR(d, 'YYYYMMDD')::INT AS date_key,
--     d AS full_date,
--     TO_CHAR(d, 'Day') AS day_of_week,
--     TO_CHAR(d, 'Month') AS month_name,
--     EXTRACT(QUARTER FROM d) AS quarter,
--     EXTRACT(YEAR FROM d) AS year,
--     CASE WHEN EXTRACT(ISODOW FROM d) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
-- FROM generate_series('2020-01-01'::date, '2030-12-31'::date, '1 day'::interval) d;

-- 🏛️ Phase 3: Architect (Professional)
-- Pre-calculating "Fiscal Periods" or "Holiday Flags" in your Date Dimension 
-- saves thousands of lines of code in your BI dashboards later.

-- 🏛️ Architect's Tip:
-- "Every Data Warehouse MUST have a Date Dimension. It allows you to 
-- compare 'This Year vs Last Year' with a simple JOIN, avoiding 
-- complex date logic in your reports."
