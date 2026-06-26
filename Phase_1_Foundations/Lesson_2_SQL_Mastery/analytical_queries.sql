-- analytical_queries.sql (The Master Template)
-- Goal: Complex Business Analysis that traditional SQL can't handle.

-- 1. RANKING: The "Top N" Pattern
-- Problem: Find the Top 2 earners in every department.
WITH RankedSales AS (
    SELECT 
        dept_id,
        name,
        salary,
        RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) as salary_rank,
        DENSE_RANK() OVER (PARTITION BY dept_id ORDER BY salary DESC) as dense_rank
    FROM employees
)
SELECT * FROM RankedSales WHERE salary_rank <= 2;

-- 2. TIME SERIES: Growth Analysis (LEAD/LAG)
-- Problem: Compare this month's revenue to last month's.
-- Why it matters: This is the #1 request from CEOs.
SELECT 
    sale_month,
    revenue,
    LAG(revenue) OVER (ORDER BY sale_month) as prev_month_revenue,
    revenue - LAG(revenue) OVER (ORDER BY sale_month) as growth,
    ROUND((revenue - LAG(revenue) OVER (ORDER BY sale_month)) / LAG(revenue) OVER (ORDER BY sale_month) * 100, 2) as growth_pct
FROM monthly_sales;

-- 3. CUMULATIVE SUMS (Running Totals)
-- Problem: See how revenue builds up over the year.
SELECT 
    sale_date,
    amount,
    SUM(amount) OVER (ORDER BY sale_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as running_total
FROM sales;

-- 4. RECURSIVE CTE: The Org Chart
-- Problem: Find the full chain of command.
WITH RECURSIVE chain AS (
    SELECT id, name, manager_id, 1 as depth
    FROM employees WHERE manager_id IS NULL
    UNION ALL
    SELECT e.id, e.name, e.manager_id, c.depth + 1
    FROM employees e
    JOIN chain c ON e.manager_id = c.id
)
SELECT * FROM chain;

-- 🏛️ Architect's Tip:
-- "Window functions are calculated in RAM. If you run them on 1 Billion rows, 
-- you might run out of memory. Always filter your data as much as possible 
-- inside the CTE (Step 1) before running the Window Function (Step 2)."
