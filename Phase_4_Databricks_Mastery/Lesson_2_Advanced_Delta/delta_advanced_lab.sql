-- delta_advanced_lab.sql
-- Architect Level: Maintenance and Performance

-- 1. THE MERGE (Upsert)
-- Imagine we have new data coming in for existing orders
CREATE TABLE IF NOT EXISTS source_updates (order_id INT, amount DOUBLE);
INSERT INTO source_updates VALUES (1, 1500.00); -- Updating the Laptop price

MERGE INTO silver_orders AS target
USING source_updates AS source
ON target.order_id = source.order_id
WHEN MATCHED THEN
  UPDATE SET target.amount = source.amount
WHEN NOT MATCHED THEN
  INSERT (order_id, amount) VALUES (source.order_id, source.amount);

-- 2. PERFORMANCE: Optimize & Z-Order
-- Z-Order helps when you frequently filter by a specific column
OPTIMIZE silver_orders ZORDER BY (order_date);

-- 3. COST SAVINGS: Vacuum
-- Remove old versions of data files to save storage cost
-- (Note: This is usually run as a scheduled maintenance task)
VACUUM silver_orders RETAIN 168 HOURS; -- Keep 7 days of history

-- 🏛️ Architect's Tip:
-- "Don't run OPTIMIZE after every single write. It's an expensive 
-- operation. Schedule it once a day or once a week during 
-- off-peak hours to keep your tables fast without overspending."
