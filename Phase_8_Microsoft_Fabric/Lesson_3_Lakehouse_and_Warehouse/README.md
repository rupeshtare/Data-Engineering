# Lesson 3: Fabric Lakehouse & Data Warehouse

> **Goal:** Understand the difference between the Fabric Lakehouse and Data Warehouse, know when to use each one, and implement the Medallion Architecture on Fabric with Delta Lake tables and the SQL Endpoint.

---

## 🏗️ Phase 1: Foundations — Lakehouse vs Warehouse in Fabric

### 1. Two Ways to Store Analytical Data in Fabric

| | Fabric Lakehouse | Fabric Data Warehouse |
|--|----------------|----------------------|
| **Storage Format** | Delta Lake (Parquet files in OneLake) | Parquet files in OneLake (Fabric-managed) |
| **Interface** | Files + Tables + SQL Endpoint (read) | Full SQL (read/write) |
| **Data Loading** | Notebooks, Pipelines, Auto-discovered Delta | Only via SQL: INSERT, COPY, Pipeline |
| **Best For** | Spark processing, flexible schema, ML data | BI-facing, strict SQL analytics, governed schemas |
| **Who Uses It** | Data Engineers (Spark notebooks) | Data Analysts (SQL only) |
| **Transactions** | ACID via Delta Lake | Full SQL ACID (T-SQL) |
| **Compute** | Shared Spark compute (Notebooks) | Dedicated Warehouse compute |
| **Primary Language** | Python/Spark + SQL (read-only via endpoint) | T-SQL (full read/write) |

**When to use Fabric Lakehouse:**
```
✅ You need Spark (Python notebooks) for complex transformations
✅ You have semi-structured data (JSON, nested arrays)
✅ Your team is ML-focused (notebooks + feature engineering)
✅ Bronze and Silver layers of Medallion
✅ Exploration and ad-hoc analysis
```

**When to use Fabric Data Warehouse:**
```
✅ Pure SQL analytics with no Spark needed
✅ Strict schemas enforced via T-SQL DDL
✅ Data Analysts who know SQL but not Python
✅ Gold layer of Medallion (final BI-facing tables)
✅ When you need full T-SQL (stored procedures, views, functions)
```

### 2. The Lakehouse SQL Endpoint — The Best of Both Worlds

Every Fabric Lakehouse **automatically generates a read-only SQL Endpoint**. This means:
-  Data Engineers write data via Spark (Notebooks)
-  Data Analysts query via SQL (no Spark knowledge needed!)
-  Power BI connects directly to the SQL Endpoint

```sql
-- Analysts can query Lakehouse tables using standard SQL via the SQL Endpoint:
-- (Connect from: Fabric SQL Endpoint, PowerBI, SSMS, Azure Data Studio)

SELECT
    FORMAT(order_date, 'yyyy-MM') AS month,
    region,
    SUM(amount)                   AS total_revenue,
    COUNT(DISTINCT customer_id)   AS unique_buyers
FROM SalesLakehouse.silver.orders     -- Lakehouse.schema.table
WHERE order_date >= '2024-01-01'
GROUP BY FORMAT(order_date, 'yyyy-MM'), region
ORDER BY month, total_revenue DESC;
```

---

## 🚀 Phase 2: Implementing Medallion on Fabric Lakehouse

### 1. Lakehouse Structure — Tables vs Files

```
SalesLakehouse/
├── Tables/                    ← Delta Lake managed tables (queryable via SQL Endpoint)
│   ├── bronze_orders          (auto-registered Delta table)
│   ├── silver_orders          (auto-registered Delta table)
│   └── gold_daily_revenue     (auto-registered Delta table)
│
└── Files/                     ← Raw file zone (not queryable via SQL without loading)
    ├── landing/               (raw CSV/JSON drops from Copy Activity)
    │   └── orders_2024-04-20.csv
    └── archive/               (moved after processing)
```

### 2. Bronze Layer — Ingest Raw Files

```python
# In a Fabric Notebook (Spark):

# Read raw CSV from the Files/ zone (landed by a Pipeline Copy Activity)
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false")  \
    .csv("Files/landing/orders_2024-04-20.csv")

# Add metadata columns
from pyspark.sql import functions as F

df_bronze = df_raw \
    .withColumn("_source_file",     F.input_file_name()) \
    .withColumn("_ingested_at",     F.current_timestamp()) \
    .withColumn("_processing_date", F.current_date())

# Write to the Tables/ zone as a Delta table (auto-registered in SQL Endpoint!)
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .saveAsTable("bronze_orders")    # <-- Available immediately in SQL Endpoint!

print(f"Bronze loaded: {df_bronze.count()} rows")
```

### 3. Silver Layer — Clean and Type

```python
# Read from bronze Delta table
df_bronze = spark.read.table("bronze_orders")

# Apply cleaning and type casting
df_silver = (
    df_bronze
    .filter(F.col("order_id").isNotNull())
    .withColumn("order_id",        F.col("order_id").cast("long"))
    .withColumn("customer_id",     F.col("customer_id").cast("long"))
    .withColumn("amount",          F.col("amount").cast("decimal(12,2)"))
    .withColumn("order_date",      F.to_date(F.col("order_date_raw"), "M/d/yyyy"))
    .withColumn("region",          F.upper(F.trim(F.col("region"))))
    .filter(F.col("amount") > 0)
    .dropDuplicates(["order_id"])
    .select("order_id", "customer_id", "amount", "region", "order_date", "_ingested_at")
)

# MERGE into silver (idempotent upsert):
from delta.tables import DeltaTable

if DeltaTable.isDeltaTable(spark, "Tables/silver_orders"):
    dt = DeltaTable.forName(spark, "silver_orders")
    dt.alias("target").merge(
        df_silver.alias("source"),
        "target.order_id = source.order_id"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    # First run: just write
    df_silver.write.format("delta").saveAsTable("silver_orders")

print(f"Silver updated: {df_silver.count()} rows")
```

### 4. Gold Layer — Aggregate for BI

```python
# Business-level aggregations for Power BI consumption
df_gold = (
    spark.read.table("silver_orders")
    .groupBy(
        F.date_trunc("month", F.col("order_date")).alias("report_month"),
        "region"
    )
    .agg(
        F.sum("amount").alias("total_revenue"),
        F.count("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_buyers"),
        F.avg("amount").alias("avg_order_value")
    )
)

# Overwrite Gold (it's a full recompute from Silver)
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_daily_revenue")

# This table is INSTANTLY available in Power BI via Direct Lake!
# No import, no data copy — Power BI reads from OneLake directly.
```

---

## 🏛️ Phase 3: Fabric Data Warehouse — SQL-First Analytics

### 1. Creating and Structuring a Warehouse

```sql
-- Everything in the Fabric Data Warehouse is standard T-SQL
-- (No Spark here — pure SQL execution)

-- Create dimension tables
CREATE TABLE gold.dim_customer (
    customer_key     INT IDENTITY(1,1) PRIMARY KEY,
    customer_id      INT NOT NULL,
    full_name        NVARCHAR(200),
    city             NVARCHAR(100),
    loyalty_tier     NVARCHAR(20),
    effective_start  DATE,
    effective_end    DATE,
    is_current       BIT DEFAULT 1
);

-- Create fact table
CREATE TABLE gold.fact_sales (
    sale_key         BIGINT IDENTITY(1,1),
    customer_key     INT REFERENCES gold.dim_customer(customer_key),
    date_key         INT,
    region           NVARCHAR(50),
    amount           DECIMAL(12,2),
    quantity         INT
);

-- Create a view for business users (hide complexity):
CREATE VIEW gold.vw_monthly_summary AS
SELECT
    FORMAT(DATEFROMPARTS(
        YEAR(d.full_date),
        MONTH(d.full_date), 1
    ), 'yyyy-MM') AS report_month,
    dc.loyalty_tier,
    dc.city,
    SUM(fs.amount) AS total_revenue,
    COUNT(DISTINCT fs.customer_key) AS unique_buyers
FROM gold.fact_sales fs
JOIN gold.dim_customer dc ON fs.customer_key = dc.customer_key
JOIN gold.dim_date     d  ON fs.date_key     = d.date_key
WHERE dc.is_current = 1
GROUP BY
    YEAR(d.full_date), MONTH(d.full_date),
    dc.loyalty_tier, dc.city;
```

### 2. Cross-Lakehouse Queries — Fabric's Superpower

```sql
-- In Fabric Data Warehouse, you can query ACROSS Lakehouses and Warehouses!
-- This is called "cross-database queries"

-- Example: Gold Warehouse joining with data from the Lakehouse SQL Endpoint
SELECT
    w.order_id,
    w.amount,
    l.customer_full_name,    -- From the Lakehouse (different item!)
    l.city
FROM MyWarehouse.gold.fact_sales w
JOIN SalesLakehouse.silver.dim_customer l   -- <-- Cross-item query!
    ON w.customer_id = l.customer_id
WHERE w.order_date = CAST(GETDATE() AS DATE);
```

### 3. Shortcuts to existing Delta tables

```sql
-- If your team already has a Lakehouse with silver_orders,
-- you can create a SHORTCUT table in the Warehouse pointing to it:

-- In the Fabric Warehouse UI: New → Table → Shortcut to OneLake table
-- This creates a read-only virtual table — no data duplication!

-- Or via SQL (preview feature):
-- CREATE EXTERNAL TABLE gold.silver_orders_shortcut
-- LOCATION '<OneLake path to Lakehouse Delta table>'
-- USING DELTA;

SELECT * FROM gold.silver_orders_shortcut LIMIT 100;
-- Reads directly from the Lakehouse's Delta files!
```

---

### 4. Direct Lake Mode — The BI Revolution
**Direct Lake Mode** is a new feature for Power BI that allows it to read Delta Parquet files directly from OneLake **without importing them into memory**.
*   **The Problem:** Import mode is fast but requires hours of "refresh" time. DirectQuery is live but slow.
*   **The Fix:** Direct Lake gives you **Import speed** (it reads the Parquet files as if they were in memory) but with **Live data access** (no refresh needed).
*   **Architect Note:** This is the default mode for all Power BI reports connected to Fabric Lakehouses/Warehouses.

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **Lakehouse vs. Warehouse:** A critical exam question: "Which item supports Spark and Python?" 
    *   **Answer:** **Lakehouse**. Warehouse is T-SQL only.
*   **SQL Analytics Endpoint:** Is the SQL Analytics Endpoint read-only or read/write?
    *   **Answer:** **Read-only**. If you need to write via SQL, you must use a **Data Warehouse** item.

### 🏢 Consultancy Scenario: "The Mixed Mode Team"
**Scenario:** A client has a team of 10 Data Engineers (Python pros) and 50 Analysts (SQL only). How do you design their Fabric workspace?
*   **Architect Answer:** **Unified Lakehouse.**
*   **The Move:** Give the DEs a **Lakehouse** where they run Spark notebooks for Bronze and Silver. Tell the Analysts to connect to the **SQL Analytics Endpoint** of that same Lakehouse. This way, everyone is looking at the exact same tables, but using the language they prefer.

### 🚀 Startup Scenario: "The Dashboard Lag"
**Scenario:** "Our Power BI dashboards are 4 hours behind because the 'Import' job takes so long. Our CEO wants live data."
*   **Answer:** **Direct Lake Mode.**
*   **The Drill:** Switch the Power BI connection from 'Import' to **Direct Lake**. Because your Gold layer is already in Delta format in OneLake, Power BI can read it instantly. The "4-hour lag" becomes a "0-second lag."

### 🏛️ FAANG Scenario: "The 1PB Partition Spike"
**Scenario:** "Our sales table is 1PB. A simple query for 'Last Month Sales' is scanning the entire 1PB. How do we fix this?"
*   **Answer:** **Partitioning and Z-Ordering.**
*   **The Drill:** In your Spark notebook, write the table with `.partitionBy("year", "month")`. In Fabric, also use the `OPTIMIZE` command with `ZORDER BY (order_date)`. This ensures the engine only reads the small folders relevant to the query, reducing scan volume from 1PB to 10GB.

---

### 🧪 Hands-on Labs
- [medallion_fabric_lab.ipynb](medallion_fabric_lab.ipynb) (A notebook demonstrating the full Bronze → Silver → Gold flow with MERGE logic)

---

### ✅ Key Takeaways
1. **Lakehouse** is for DEs (Spark); **Warehouse** is for Analysts (T-SQL).
2. **OneLake** ensures both see the same data in Delta format.
3. **SQL Endpoint** is the bridge for SQL analysts to query Spark tables.
4. **Direct Lake** is why you use Fabric for BI—no more refresh waiting.
5. **MERGE** is your primary tool for idempotent data pipelines.
6. **Cross-database queries** break the silos between different workspaces and projects.

[Next: Lesson 4: Notebooks & Spark (Coding in the Cloud) →](../Lesson_4_Notebooks_and_Spark/README.md)

---

## 🧪 Practice Exercises

### Exercise 1 — Full Medallion Pipeline on the Titanic Dataset (Beginner)
**Goal:** Implement Bronze → Silver → Gold on real data using three notebooks.

```python
# NOTEBOOK 1: "01_bronze_ingest"
# (Assumes titanic.csv was already landed in Files/landing/ by a Pipeline — see Lesson 2 Exercise 1)

from pyspark.sql import functions as F

# Read raw CSV
df_raw = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv("Files/landing/titanic.csv")

# Add audit columns
df_bronze = df_raw \
    .withColumn("_source_file",     F.input_file_name()) \
    .withColumn("_ingested_at",     F.current_timestamp()) \
    .withColumn("_processing_date", F.current_date())

# Write to Delta table
df_bronze.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("bronze_titanic")

print(f"Bronze rows: {df_bronze.count()}")

# ──────────────────────────────────────────────────────────────────────────────

# NOTEBOOK 2: "02_silver_clean"

df_bronze = spark.read.table("bronze_titanic")

df_silver = (
    df_bronze
    .filter(F.col("PassengerId").isNotNull())
    .withColumn("PassengerId", F.col("PassengerId").cast("int"))
    .withColumn("Survived",    F.col("Survived").cast("int"))
    .withColumn("Pclass",      F.col("Pclass").cast("int"))
    .withColumn("Age",         F.col("Age").cast("double"))
    .withColumn("Fare",        F.col("Fare").cast("double"))
    .withColumn("Name",        F.trim(F.col("Name")))
    .withColumn("Sex",         F.lower(F.trim(F.col("Sex"))))
    .filter(F.col("Fare") >= 0)
    .dropDuplicates(["PassengerId"])
    .select("PassengerId","Survived","Pclass","Name","Sex","Age","Fare","Embarked","_ingested_at")
)

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("silver_titanic")

print(f"Silver rows (after cleaning): {df_silver.count()}")

# ──────────────────────────────────────────────────────────────────────────────

# NOTEBOOK 3: "03_gold_aggregate"

df_silver = spark.read.table("silver_titanic")

df_gold = (
    df_silver
    .groupBy("Pclass", "Sex")
    .agg(
        F.count("PassengerId").alias("total_passengers"),
        F.sum("Survived").alias("total_survived"),
        F.round(F.avg("Survived") * 100, 1).alias("survival_rate_pct"),
        F.round(F.avg("Fare"), 2).alias("avg_fare"),
        F.round(F.avg("Age"), 1).alias("avg_age")
    )
    .orderBy("Pclass", "Sex")
)

df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("gold_survival_summary")

display(df_gold)
# Expected: 6 rows (3 classes × 2 genders) showing survival rates by class and gender
```

**Check your work:**
```sql
-- Open the SQL Endpoint of your Lakehouse and run:
SELECT Pclass, Sex, survival_rate_pct, avg_fare
FROM gold_survival_summary
ORDER BY survival_rate_pct DESC;

-- Expected insight: 1st class women had the highest survival rate (~97%)
```

---

### Exercise 2 — Delta MERGE Idempotency Test (Intermediate)
**Goal:** Prove that the MERGE pattern is safe to run multiple times (idempotent).

```python
# Step 1: Run the silver notebook once → silver_titanic has N rows
count_before = spark.read.table("silver_titanic").count()
print(f"Before 2nd run: {count_before} rows")

# Step 2: Run the silver notebook AGAIN (simulate a pipeline re-run)
# (Re-execute Notebook 2 from Exercise 1 — but this time use MERGE instead of overwrite)

from delta.tables import DeltaTable
df_silver = spark.read.table("bronze_titanic")  # re-read source

# Apply same cleaning transformations as before...
# (same code as notebook 2 above)

# MERGE instead of overwrite:
if DeltaTable.isDeltaTable(spark, "Tables/silver_titanic"):
    dt = DeltaTable.forName(spark, "silver_titanic")
    dt.alias("target").merge(
        df_silver.alias("source"),
        "target.PassengerId = source.PassengerId"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
else:
    df_silver.write.format("delta").saveAsTable("silver_titanic")

count_after = spark.read.table("silver_titanic").count()
print(f"After 2nd run: {count_after} rows")

# Expected: count_before == count_after → no duplicates created!
# This proves idempotency: safe to re-run on failure/retry.
```

---

### Exercise 3 — Cross-Lakehouse SQL Query (Advanced)
**Goal:** Query two separate Lakehouses in a single SQL statement.

```
Setup:
1. Create a second Lakehouse: "ReferenceLakehouse"
2. In a Notebook, create a small reference table there:

   spark.sql("""
     CREATE TABLE region_map
     USING DELTA AS
     SELECT 'C' AS embarked_code, 'Cherbourg'   AS port_name UNION ALL
     SELECT 'Q',                  'Queenstown'              UNION ALL
     SELECT 'S',                  'Southampton'
   """)

3. Now open the SQL Endpoint of your LearningLakehouse and run:

   SELECT
       s.Pclass,
       r.port_name,
       COUNT(s.PassengerId)          AS passengers,
       SUM(s.Survived)               AS survived,
       ROUND(AVG(s.Survived)*100, 1) AS survival_rate
   FROM LearningLakehouse.dbo.silver_titanic s         -- your main lakehouse
   JOIN ReferenceLakehouse.dbo.region_map   r          -- cross-lakehouse join!
     ON s.Embarked = r.embarked_code
   GROUP BY s.Pclass, r.port_name
   ORDER BY s.Pclass, survival_rate DESC;

Expected: Results broken down by embarkation port AND passenger class,
with port names (not raw codes) from your separate reference Lakehouse.
```

---

## 💼 Common Interview Questions

**Q1: What is the difference between a Fabric Lakehouse and a Fabric Data Warehouse?**
> **Lakehouse**: Stores data as Delta Lake Parquet files in OneLake. Supports Spark notebooks (Python/Scala) for transformation. Provides a **read-only** SQL Endpoint for analysts. Best for Bronze and Silver layers, data engineers, and ML workloads. **Data Warehouse**: Stores data in Fabric-managed Parquet (also in OneLake). Supports **full read/write T-SQL** — stored procedures, views, INSERT, UPDATE, DELETE. Best for Gold layer, BI-facing clean schemas, and SQL-only analyst teams. The key difference: full SQL write access.

**Q2: What is the Lakehouse SQL Endpoint and why is it powerful?**
> Every Fabric Lakehouse automatically exposes a **read-only SQL Endpoint** that lets analysts query Delta tables using standard SQL — without knowing Spark or Python. A data engineer writes data with a Spark notebook (`saveAsTable()`), and immediately a business analyst can run `SELECT * FROM silver_orders` via SSMS, Azure Data Studio, or Power BI. No duplication, no ETL job to "export for SQL" — the same Delta files serve both workloads.

**Q3: Why is the MERGE pattern important for Silver layer writes?**
> In a production pipeline, failures and retries happen. If you use `mode("overwrite")`, a partial run erases good data. If you use `mode("append")`, a retry creates duplicates. **MERGE** (Delta Lake upsert) is idempotent: if the record already exists (matched on `order_id`), it updates it. If it's new, it inserts it. Running the same MERGE twice produces the same result — no duplicates, no data loss. This "safe to re-run" property is essential for reliable pipelines.

**Q4: Explain the Medallion Architecture layers: Bronze, Silver, Gold.**
> **Bronze**: Raw data as-is from the source. Append-only, never modified. Includes audit columns (`_ingested_at`, `_source_file`). Purpose: full audit trail, re-processable. **Silver**: Cleaned, typed, deduplicated, conformed data. MERGE (upsert) pattern. Row filters (remove nulls/negatives), type casts, standardized values. Purpose: trusted, usable data for analysts and ML. **Gold**: Business-level aggregations built from Silver. Joins, groupings, KPIs. Used directly by Power BI. Purpose: query performance + business-friendliness.

**Q5: What are cross-item queries in Fabric and why are they useful?**
> In Fabric, you can write a single SQL query that joins tables from **different Lakehouses or Warehouses** within the same tenant: `FROM LakehouseA.dbo.table1 JOIN WarehouseB.dbo.table2`. This eliminates the need to physically copy reference data (e.g., a lookup/dimension table) into every Lakehouse that needs it. One Lakehouse can own the canonical dimension, and all other items query it directly — improving data consistency and eliminating ETL complexity.

