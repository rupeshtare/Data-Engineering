-- fact_dim_lab.sql
-- Phase 2, Lesson 5: Fact & Dimension Deep Dive
-- Goal: Understanding Grain and Types of Facts.

-- 🏗️ Phase 1: Absolute Foundations (Beginner)
-- Defining the "Grain": What does one row represent?
-- Grain: One row = One line item in a sales receipt.

-- 🚀 Phase 2: Intermediate (Developer)
-- Types of Facts:
-- 1. Additive: Can be summed (e.g., Sales Amount)
-- 2. Non-Additive: Cannot be summed across any dimension (e.g., Ratio, Temperature)
-- 3. Semi-Additive: Can be summed across some (e.g., Account Balance - sum across customers, not time)

CREATE TABLE fact_inventory (
    inventory_key SERIAL PRIMARY KEY,
    date_key INT REFERENCES dim_date(date_key),
    product_key INT,
    stock_on_hand INT, -- Semi-Additive (Sum by product, not by date)
    unit_cost DECIMAL(10,2) -- Non-Additive
);

-- 🏛️ Phase 3: Architect (Professional)
-- Designing for "Factless Fact Tables":
-- A table that records an EVENT happened (e.g., Student Attendance). 
-- There is no "Amount", just keys. Use COUNT(*) to get metrics.

-- 🏛️ Architect's Tip:
-- "Always define your Grain before you write a single line of SQL. 
-- If your Grain is inconsistent, your sums will be wrong, and 
-- leadership will lose trust in your data."
