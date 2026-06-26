-- advanced_indexing.sql (The Master Template)
-- Goal: Designing databases for fast searching at Terabyte scale.

-- 1. THE BASICS: Single Column Index
-- Use this for IDs or Emails that you search for 90% of the time.
CREATE INDEX IF NOT EXISTS idx_learner_name ON learners(name);

-- 2. THE ARCHITECT MOVE: Composite Index (Multiple Columns)
-- Problem: You often search for people by BOTH Department AND Start Date.
-- Solution: One index that covers both. Order matters! (Put the most filtered first).
CREATE INDEX IF NOT EXISTS idx_dept_date ON learners(dept_id, started_at);

-- 3. THE "COVERING" INDEX
-- Problem: You want to count learners in a department without touching the massive table.
-- Solution: Include the count column in the index itself.
CREATE INDEX IF NOT EXISTS idx_dept_only ON learners(dept_id) INCLUDE (track);

-- 4. VERIFICATION: Is the Index working?
-- Use EXPLAIN to see the database's "Plan".
-- Look for 'Index Scan' (Good) vs 'Seq Scan' (Bad/Search Everyone).

-- Run this to see the plan without executing:
EXPLAIN SELECT name FROM learners WHERE name = 'Alex Fresher';

-- Run this to see the ACTUAL timing:
EXPLAIN ANALYZE SELECT name FROM learners WHERE name = 'Alex Fresher';

-- 🏛️ Architect's Tip: 
-- "Indexing a column with only 2 values (like Male/Female) is a waste. 
-- The database will just ignore it and scan the whole table anyway. 
-- Only index columns with high 'Cardinality' (many unique values)."
