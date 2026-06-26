# Lesson 4: Databricks SQL & Photon Engine (The Master Guide)

> **Goal:** Master Databricks SQL Warehouses and the Photon engine to deliver sub-second BI query performance, build self-service dashboards for business users, and configure alerting — all without leaving the Databricks platform.

---

## 🏗️ Phase 1: Foundations — What is Databricks SQL?

### 1. Databricks SQL vs Notebooks

| | Notebooks | Databricks SQL |
|--|-----------|---------------|
| **Audience** | Data Engineers, Data Scientists | Business Analysts, BI Users |
| **Compute** | All-Purpose / Job Clusters | SQL Warehouses (Photon-optimized) |
| **Language** | Python, SQL, Scala, R | SQL only |
| **Output** | Code + results inline | Tables, Charts, Dashboards |
| **Scheduling** | Databricks Workflows | Databricks SQL Alerts |
| **Cost Model** | DBUs per hour | DBUs per query (Serverless) |

**Who uses Databricks SQL in real companies:**
-  Finance Analysts querying revenue tables
-  Product Managers checking feature adoption dashboards
-  Operations teams monitoring SLA metrics
-  Any team using PowerBI / Tableau / Looker connected to Databricks

### 2. SQL Warehouse Types — Choosing the Right One

```
Three SQL Warehouse types:

┌─────────────────────────────────────────────────────────────────────┐
│ SERVERLESS SQL WAREHOUSE (2024 Recommended)                          │
│ ✅ Startup: 2-4 seconds (vs 5-10 min for classic!)                  │
│ ✅ No cluster management — fully managed by Databricks               │
│ ✅ Pay per query second (not per cluster hour)                       │
│ ✅ Auto-scales up and down instantly                                 │
│ ✅ Photon engine included                                            │
│ Use: All new projects, BI-facing workloads                           │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ PRO SQL WAREHOUSE                                                     │
│ ✅ Photon engine included                                            │
│ ✅ Can use Spot instances (cost saving)                              │
│ ⚠️  5-8 minute startup time                                         │
│ Use: Long-running ETL or when Serverless isn't available             │
└─────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────┐
│ CLASSIC SQL WAREHOUSE (Legacy)                                        │
│ ❌ No Photon engine                                                  │
│ ❌ Slower                                                            │
│ Use: Only if existing workloads are already on classic               │
└─────────────────────────────────────────────────────────────────────┘
```

---

## 🚀 Phase 2: The Photon Engine — Why It's Game-Changing

### 1. What is Photon?

**Photon** is Databricks' proprietary, native vectorized query engine written in **C++**. It replaces the standard Spark Java/JVM execution for SQL and DataFrame operations.

```
Standard Spark Execution (JVM):
→ Java bytecode → JVM interpretation → CPU execution
→ Processes data ROW BY ROW within each task
→ JVM overhead + Garbage Collection pauses

Photon Execution (C++):
→ Native C++ machine code → CPU execution directly
→ Processes data in COLUMN VECTORS (512-1024 rows at once!)
→ Uses SIMD CPU instructions (process 8 values per CPU cycle vs 1)
→ No GC pauses, no JVM overhead
→ Result: 2-8x faster SQL, sometimes 10x for aggregations
```

### 2. When Photon Helps Most

```sql
-- Photon MASSIVELY accelerates these operations:

-- 1. Aggregations over large tables (SUM, AVG, COUNT DISTINCT)
SELECT region, SUM(amount), AVG(amount), COUNT(DISTINCT customer_id)
FROM fact_sales                    -- e.g., 1 billion rows
GROUP BY region;
-- Without Photon: 45 seconds
-- With Photon:     8 seconds (5.6x faster!)

-- 2. Hash joins between large tables
SELECT fs.*, dp.category, dc.city
FROM fact_sales fs
JOIN dim_product dp ON fs.product_key = dp.product_key
JOIN dim_customer dc ON fs.customer_key = dc.customer_key;
-- Photon uses vectorized hash join → much faster than Spark SortMergeJoin

-- 3. String operations (LIKE, REGEXP, TRIM, UPPER)
SELECT UPPER(TRIM(region)), REGEXP_REPLACE(phone, '[^0-9]', '') FROM customers;
-- SIMD string operations → 8x faster than JVM string handling

-- 4. Complex GROUP BY with ROLLUP / CUBE
SELECT city, region, year, SUM(amount)
FROM fact_sales
GROUP BY ROLLUP(city, region, year);
```

### 3. Photon Limitations — Know What It Doesn't Optimize

```python
# Photon DOES NOT accelerate:
# ❌ Python UDFs (User Defined Functions) — Python is not C++
#    → Avoid Python UDFs in production SQL! Use built-in SQL functions instead.
# ❌ pandas UDFs (scalar only) — some acceleration in newer versions
# ❌ RDD operations — use DataFrame/SQL API instead

# BAD: Python UDF kills Photon performance
from pyspark.sql.functions import udf
@udf("string")
def categorize_spend(amount):    # Python UDF — breaks vectorization!
    if amount > 10000: return "High"
    elif amount > 1000: return "Medium"
    return "Low"

# GOOD: Native SQL expression — Photon fully vectorized
from pyspark.sql.functions import when, col
df.withColumn("spend_tier",
    when(col("amount") > 10000, "High")
    .when(col("amount") > 1000, "Medium")
    .otherwise("Low")
)
```

---

## 🏛️ Phase 3: Databricks SQL Features for Production

### 1. Databricks SQL Query Editor — Professional Tips

```sql
-- Tip 1: Named parameters (reusable queries for different dates)
SELECT *
FROM fact_sales
WHERE order_date = {{ date_param }}         -- Named parameter (filled in UI)
  AND region = {{ region_param }};

-- Tip 2: Query snippets (save reusable SQL fragments)
-- Save as snippet "date_filter":
-- WHERE order_date BETWEEN date_add(current_date(), -30) AND current_date()

-- Tip 3: Variable substitution in multiple places
SELECT
    customer_id,
    SUM(amount) AS total_spend
FROM fact_sales
WHERE order_date >= '{{ start_date }}'
  AND order_date <= '{{ end_date }}'
GROUP BY customer_id
HAVING total_spend > {{ min_spend }};

-- Tip 4: GROUP BY ALL (Databricks SQL-only shorthand)
SELECT date_trunc('month', order_date), region, SUM(amount)
FROM fact_sales
GROUP BY ALL;   -- No need to list all GROUP BY columns manually!

-- Tip 5: UNPIVOT (great for reshaping wide tables)
SELECT region, metric_name, metric_value
FROM (SELECT region, total_revenue, total_orders, avg_order_value FROM gold_summary)
UNPIVOT (
    metric_value FOR metric_name IN (total_revenue, total_orders, avg_order_value)
);
```

### 2. Dashboards and Visualizations

```
Building a Production Dashboard in Databricks SQL:

Step 1: Create individual SQL queries in the Query Editor
  • "Daily Revenue Trend" → Line chart
  • "Revenue by Region" → Bar chart
  • "Top 10 Customers" → Table with conditional formatting
  • "Revenue vs Target" → Counter with red/green coloring

Step 2: Add to Dashboard
  Dashboard → New Dashboard → Add Widgets
  Each widget uses a saved query as its data source

Step 3: Configure Refresh
  Dashboard Settings → Schedule → Every 1 hour
  (Databricks auto-reruns all queries on schedule)

Step 4: Share with stakeholders
  Dashboard → Share → Add users/groups
  They see a live, always-up-to-date dashboard without any code!
```

### 3. Alerts — Automated Threshold Monitoring

```sql
-- Create alerts from SQL queries:
-- Alert fires when the query result meets a condition

-- Example 1: Alert when sales drop > 20% vs yesterday
WITH today AS (
    SELECT SUM(amount) AS revenue FROM fact_sales WHERE order_date = current_date()
),
yesterday AS (
    SELECT SUM(amount) AS revenue FROM fact_sales WHERE order_date = date_add(current_date(), -1)
)
SELECT
    today.revenue AS today_revenue,
    yesterday.revenue AS yesterday_revenue,
    (today.revenue - yesterday.revenue) / yesterday.revenue * 100 AS pct_change
FROM today, yesterday;

-- Alert configuration:
-- Column: pct_change
-- Condition: less than -20
-- Frequency: Every 1 hour
-- Notification: Email + Slack webhook

-- Example 2: Alert when pipeline data becomes stale
SELECT
    MAX(ingested_at) AS last_ingestion,
    TIMESTAMPDIFF(HOUR, MAX(ingested_at), CURRENT_TIMESTAMP()) AS hours_stale
FROM silver.orders;
-- Alert: hours_stale > 2 → "Pipeline may be stuck!"

-- Example 3: Alert when a Gold table has too few rows (pipeline failure signal)
SELECT COUNT(*) AS row_count
FROM gold.fact_daily_sales
WHERE report_date = current_date();
-- Alert: row_count < 1000 → "Today's data may not have loaded"
```

### 4. Connecting BI Tools to Databricks SQL

```
PowerBI Desktop → Databricks Connector:
1. Get Data → Azure → Azure Databricks
2. Server Hostname: <workspace>.azuredatabricks.net
3. HTTP Path: /sql/1.0/warehouses/<warehouse-id>
4. Authentication: Personal Access Token (from Databricks UI → Settings → Tokens)

Tableau Desktop:
1. Connect → To a Server → Databricks
2. Server: <workspace>.azuredatabricks.net
3. HTTP Path: /sql/1.0/warehouses/<warehouse-id>
4. Token: <Personal Access Token>

Looker / Looker Studio:
1. Add Connection → Databricks
2. JDBC URL format

# Best practices for BI tool connections:
# 1. Use a SERVICE PRINCIPAL (not personal token!) for production
# 2. Create a dedicated SQL Warehouse for BI tools (separate from ETL!)
# 3. Use Unity Catalog permissions to limit what each BI user can see
# 4. Enable Query Watchdog to kill runaway queries from analysts

# Enabling Query Watchdog (protect the warehouse):
# SQL Warehouse → Edit → Query Execution Time Limit → Set 5 minutes
# → Any query running more than 5 minutes is automatically killed
```

### 5. Query History and Cost Attribution

```sql
-- Databricks SQL exposes a system table for query history!
-- (Available in Unity Catalog system schema)

-- Top 10 most expensive queries this week:
SELECT
    statement_text,
    user_name,
    total_duration_ms / 1000.0          AS duration_seconds,
    rows_produced,
    bytes_read_remote / 1e9             AS gb_read,
    warehouse_id
FROM system.query.history
WHERE start_time >= date_sub(current_date(), 7)
  AND statement_type = 'SELECT'
ORDER BY total_duration_ms DESC
LIMIT 10;

-- Cost by user (for chargebacks):
SELECT
    user_name,
    COUNT(*)                            AS total_queries,
    SUM(total_duration_ms) / 3600000.0 AS total_hours,
    AVG(total_duration_ms) / 1000.0    AS avg_duration_seconds
FROM system.query.history
WHERE start_time >= date_trunc('month', current_date())
GROUP BY user_name
ORDER BY total_hours DESC;
```

---

### 6. AI Functions — The Next Frontier of SQL
Databricks SQL now includes built-in AI functions that allow you to use Large Language Models (LLMs) directly in your SELECT statements.
*   **Sentiment Analysis:** `SELECT ai_analyze_sentiment(comment) FROM reviews;`
*   **Translation:** `SELECT ai_translate(text, 'es') FROM comments;`
*   **General AI:** `SELECT ai_gen('Summarize this text: ' || long_description);`

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Data Analyst Certification Drill
*   **Serverless Pricing:** Understand that Serverless SQL Warehouses have a **Startup** period where Databricks keeps a "warm pool" of compute. You are only billed for the time your warehouse is actually running queries.
*   **Query Profile:** Know how to read a Query Profile. If you see a **red node** with "Spilling to Disk," it means your cluster's RAM is too small for the join/sort operation.

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **SQL Analytics Endpoint:** Fabric provides a T-SQL endpoint for its Lakehouses. For the exam, know that while Fabric uses **T-SQL** (SQL Server syntax), Databricks uses **Spark SQL** (Standard ANSI SQL). This impacts how you write string functions and window operations.

### 🏢 Consultancy Scenario: "The Snowflake vs. Databricks Bake-off"
**Scenario:** A client is deciding between Snowflake and Databricks SQL. They want to know which is "faster."
*   **Architect Answer:** **"It depends on the data format and type of query."**
*   **The Move:** Explain that Databricks SQL (with Photon) is often faster for **large-scale ETL and complex joins** on open data lakes. Snowflake is often faster for **small concurrency, highly-cached BI queries**. Suggest a **Benchmark** using the client's actual data, not generic TPCDS results.

### 🚀 Startup Scenario: "Tableau is too expensive!"
**Scenario:** Your startup can't afford $1,000/month for Tableau. How do you give dashboards to your 10 employees?
*   **Answer:** **Use Databricks SQL Dashboards and Alerts.**
*   **The Drill:** Databricks SQL Dashboards are "Free" (included with your compute). You can build high-quality charts, schedule them to refresh, and even set up **Slack Alerts** for when KPIs drop. This saves the startup $12k/year in BI licensing fees.

### 🏛️ FAANG Scenario: "The 100-Table BI Warehouse"
**Scenario:** "Our BI warehouse has 100 tables and 500 active analysts. We are seeing 'Queueing' in our SQL Warehouse. How do we scale?"
*   **Answer:** **Enable Multi-cluster Load Balancing.**
*   **The Drill:** Don't just make the warehouse larger (Scale Up). Set `Max Clusters: 10` (Scale Out). Databricks will automatically spin up identical warehouses to handle the concurrent users. Also, enable **Result Caching** so the 2nd analyst asking the same question gets the answer in 0.1 seconds from the cache.

---

### 🧪 Hands-on Labs
- [photon_benchmark_lab.sql](photon_benchmark_lab.sql) (Compare standard Spark vs. Photon performance)

---

### ✅ Key Takeaways
1. **Photon** is the C++ engine that makes the Lakehouse as fast as a Warehouse.
2. **Serverless SQL** is the modern standard — zero management, 2-second start.
3. **AI Functions** bring LLMs to your data without a single line of Python.
4. **Query Profile** is your primary tool for finding "vampire queries" that drink your budget.
5. **Dashboarding** in Databricks SQL is a "free" alternative to expensive BI tools.
6. **Result Caching** is critical for high-concurrency BI workloads.

[Next: Lesson 5: Unity Catalog Deep Dive (Data Governance) →](../Lesson_5_Unity_Catalog_Deep_Dive/README.md)

---

## 🧪 Practice Exercises

### Exercise 1 — Photon Performance Test (Beginner)
**Goal:** Prove that Photon is faster for complex aggregations.

```sql
-- 1. Create a large dummy table (run this in a SQL Warehouse)
CREATE TABLE main.default.photon_test AS 
SELECT id, rand() as val, 'Category-' || (id % 100) as cat 
FROM range(10000000); -- 10 million rows

-- 2. Run a complex aggregation with Photon OFF
-- (You can toggle Photon in the SQL Warehouse settings)
SELECT cat, avg(val), sum(val) 
FROM main.default.photon_test 
GROUP BY cat;
-- Note the execution time: 1.2s

-- 3. Run with Photon ON
SELECT cat, avg(val), sum(val) 
FROM main.default.photon_test 
GROUP BY cat;
-- Note the execution time: 0.3s (approx 4x speedup!)
```

---

### Exercise 2 — SQL Alerts for Operations (Intermediate)
**Goal:** Create an automated alert that fires when a condition is met.

```sql
-- 1. Create a query that checks for high-value orders
-- Saved Query Name: "HighValueAlert"
SELECT count(*) as alert_count
FROM gold.fact_sales
WHERE amount > 10000 
  AND order_date >= date_add(current_date(), -1);

-- 2. Create the Alert (UI: SQL → Alerts → Create Alert)
--    Query: "HighValueAlert"
--    Trigger condition: alert_count > 0
--    Refresh frequency: Every 1 hour
--    Action: Send Email / Slack
```

---

### Exercise 3 — Usage Analysis via Information Schema (Architect)
**Goal:** Identify which tables are taking up the most storage in your catalog.

```sql
-- Query the Unity Catalog information_schema
SELECT 
    table_schema, 
    table_name, 
    (data_size / 1024 / 1024) as size_mb
FROM main.information_schema.tables 
WHERE table_schema != 'information_schema'
ORDER BY data_size DESC
LIMIT 10;

-- Question: Why is it important to monitor 'dirty' tables that are never queried?
-- Answer: Storage is cheap, but these tables clutter the catalog and increase lineage complexity.
```

---

## 💼 Common Interview Questions

**Q1: What is Photon and how does it relate to Apache Spark?**
> **Photon** is a vectorized execution engine written in C++ that is 100% compatible with Spark APIs. It doesn't replace Spark; it replaces Spark's **execution layer** for the most expensive operators (joins, aggregations). It is much faster because it uses SIMD (Single Instruction, Multiple Data) CPU instructions and avoids the overhead of the Java Virtual Machine.

**Q2: What are the benefits of a Serverless SQL Warehouse over an All-Purpose Cluster for BI?**
> (1) **Startup Time**: Starts in <10 seconds compared to 5-10 minutes. (2) **Cost**: You only pay for the exact seconds a query is running (per-second billing). (3) **Optimization**: Serverless warehouses are pre-tuned for BI concurrency; they handle 100 people clicking on a dashboard better than a standard Spark cluster.

**Q3: How does Databricks SQL handle "Scaling"?**
> It uses **Multi-Cluster Load Balancing**. If you have 50 users and your warehouse is getting slow, Databricks automatically spins up a second, third, or fourth cluster in parallel to handle the queue. Once the users go home, it shuts down the extra clusters to save cost.

**Q4: What is the purpose of the `Query History` page?**
> The Query History page is used for **Performance Tuning** and **Audit**. You can see exactly who ran which query, how long it took, what the execution plan looked like (to find bottlenecks like scans), and whether the query used the cache.

**Q5: What is 'Data Skipping' in Databricks SQL?**
> When you query a Delta table, Databricks SQL doesn't read every file. It looks at the **Delta Log metadata** (which stores min/max values for every column in every file). If your query is `WHERE id = 500` and a file says its values only go from `1` to `100`, Databricks skips that file entirely. This reduces I/O and speeds up queries tremendously.
