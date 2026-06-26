-- scd_lab.sql
-- Phase 2, Lesson 6: SCD (Slowly Changing Dimensions)
-- Goal: Tracking history when values change.

-- 🏗️ Phase 1: Absolute Foundations (Beginner)
-- SCD Type 1: Overwrite (No history)
-- If a customer moves from NY to CA, we just change 'NY' to 'CA'. 
-- We "lose" the information that they were ever in NY.

-- 🚀 Phase 2: Intermediate (Developer)
-- SCD Type 2: Add New Row (Full history)
-- This is the gold standard for Data Engineering.

CREATE TABLE dim_customer_history (
    customer_key SERIAL PRIMARY KEY,
    customer_id INT, -- Original Business ID
    name TEXT,
    address TEXT,
    start_date DATE,
    end_date DATE, -- NULL means currently active
    is_current BOOLEAN
);

-- 🏛️ Phase 3: Architect (Professional)
-- Implementation Logic:
-- 1. Expire the old record (Set end_date = today, is_current = FALSE)
-- 2. Insert the new record (Set start_date = today, is_current = TRUE)

-- 🏛️ Architect's Tip:
-- "SCD Type 2 is the secret to 'Point-in-Time' reporting. It allows 
-- you to answer: 'What was the customer's address at the time 
-- they made this purchase?' without incorrectly using their 
-- current address."
