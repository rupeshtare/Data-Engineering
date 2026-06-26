-- basic_sql_lab.sql (The Master Template)
-- Goal: Absolute Zero to Basic Data management.

-- 1. FOUNDATIONS: What is a table?
-- A table is a collection of related data.
-- We use 'IF NOT EXISTS' to make the script safe to run many times.

CREATE TABLE IF NOT EXISTS departments (
    dept_id INT PRIMARY KEY,
    dept_name VARCHAR(50) NOT NULL
);

CREATE TABLE IF NOT EXISTS learners (
    id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    track VARCHAR(50),
    dept_id INT,
    started_at DATE DEFAULT CURRENT_DATE,
    FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
);

-- 2. DML: Moving data in (INSERT)
-- Multi-row insert: Faster and cleaner.
INSERT INTO departments (dept_id, dept_name) VALUES 
(10, 'Data Engineering'),
(20, 'Data Science');

INSERT INTO learners (id, name, track, dept_id, started_at) VALUES 
(1, 'Alex Fresher', 'DE Roadmap', 10, '2024-03-19'),
(2, 'Sam Junior', 'Spark Mastery', 10, '2024-03-20'),
(3, 'Chris Analyst', 'BI Tools', 20, '2024-03-21');

-- 3. DQL: Asking basic questions
-- Simple SELECT
SELECT * FROM learners;

-- Filtering with logic (WHERE, AND, OR)
SELECT name FROM learners 
WHERE track = 'DE Roadmap' AND dept_id = 10;

-- 4. HOUSEKEEPING: Changing structures (ALTER)
-- Adding a column later
ALTER TABLE learners ADD COLUMN email VARCHAR(100);

-- 5. THE JOIN (Connecting tables)
SELECT l.name, d.dept_name 
FROM learners l 
INNER JOIN departments d ON l.dept_id = d.dept_id;

-- 🏛️ Architect's Tip:
-- "Start every project by defining your 'Foreign Keys'. 
-- This ensures that you can't have a learner in a department that 
-- doesn't exist. This is called 'Referential Integrity'."
