# Lesson 4: Notebooks & Spark in Microsoft Fabric

> **Goal:** Master Fabric's built-in Spark environment — write, run, and optimize PySpark notebooks without managing any infrastructure. Learn library management, Spark pools, and the best patterns for performant data processing pipelines.

---

## 🏗️ Phase 1: Fabric Spark — What Makes It Different

### 1. Why Fabric Spark Requires No Setup

In traditional Spark (Databricks, EMR, HDInsight) you must:
- Create a cluster → choose instance type, node count, autoscale settings
- Wait for cluster to start (3–10 minutes)
- Pay for idle time even when no notebooks are running
- Manage Spark version upgrades yourself

**In Fabric, all of that is gone:**

```
Traditional Spark:                     Fabric Spark:
────────────────────────────────────   ────────────────────────────────────
1. Create cluster (5 min wait)    →    1. Open a notebook → click ▶ Run
2. Attach notebook to cluster     →    (Fabric auto-provisions Spark, attaches it)
3. Install libraries on cluster   →    2. Declare libraries in environment
4. Monitor cluster health         →    (Fabric manages the rest)
5. Remember to terminate cluster  →    (Fabric auto-terminates after idle timeout)

You manage: EVERYTHING              You manage: NOTHING (just write code)
```

**Fabric uses a "Session-Based" Spark model:**
- When you run a notebook cell, Fabric spins up a Spark session in seconds
- The session stays alive while you are active
- After ~20 minutes of inactivity, it pauses automatically (saving CUs)
- All compute is charged against your Fabric Capacity (F-SKU)

---

### 2. Fabric Spark Runtimes

Microsoft ships tested Spark Runtime versions. Always pin a runtime for production:

| Runtime | Apache Spark | Python | Delta Lake | Default As Of |
|---------|-------------|--------|-----------|--------------|
| **Runtime 1.1** | Spark 3.3 | 3.10 | 2.2 | GA (2023) |
| **Runtime 1.2** | Spark 3.4 | 3.11 | 2.4 | GA (2024) |
| **Runtime 1.3** | Spark 3.5 | 3.11 | 3.1 | GA (2024-H2) |

```
How to change runtime for a Lakehouse/Workspace:
Workspace Settings → Data Engineering / Science → Spark Settings → Runtime version
```

> 💡 **Architect's tip:** Pin Runtime **1.2** or **1.3** for production. Avoid "Latest" in production pipelines — a runtime upgrade can break library compatibility overnight.

---

## 🚀 Phase 2: Writing Notebooks in Fabric

### 1. Notebook Anatomy

```
A Fabric Notebook is made of cells:
┌──────────────────────────────────────────────────────────────────┐
│ [Markdown Cell]  # Bronze Layer — Raw Ingestion                  │
│  ↑ Documentation cells — use these to explain your pipeline!     │
├──────────────────────────────────────────────────────────────────┤
│ [Code Cell — Python]                                             │
│  df = spark.read.csv("Files/landing/orders.csv", header=True)   │
│  display(df)         ← Fabric's built-in beautiful table preview │
├──────────────────────────────────────────────────────────────────┤
│ [Code Cell — SQL]  (%%sql magic command)                         │
│  %%sql                                                           │
│  SELECT region, SUM(amount) FROM silver_orders GROUP BY region   │
├──────────────────────────────────────────────────────────────────┤
│ [Code Cell — Scala / R]  (multi-language support)                │
│  %%scala                                                         │
│  val df = spark.read.parquet("Files/data.parquet")               │
└──────────────────────────────────────────────────────────────────┘
```

### 2. The `notebookutils` API — Fabric's Built-In SDK

`notebookutils` (also called `mssparkutils` in Synapse) is Fabric's Python API for interacting with the platform from inside a notebook:

```python
# ── File System (Files in OneLake) ──────────────────────────────────
notebookutils.fs.ls("Files/landing/")           # List files in Lakehouse Files zone
notebookutils.fs.cp("Files/a.csv", "Files/b/")  # Copy a file
notebookutils.fs.mv("Files/a.csv", "Files/archive/a.csv")  # Move (archive after processing)
notebookutils.fs.rm("Files/tmp/", recurse=True)  # Delete temp files

# ── Secrets (Azure Key Vault) ─────────────────────────────────────
# NEVER hardcode passwords in notebooks! Use Key Vault:
db_password = notebookutils.credentials.getSecret(
    "https://my-keyvault.vault.azure.net/",
    "sql-db-password"
)
# db_password now holds the secret value securely

# ── Run Another Notebook ──────────────────────────────────────────
# Call a child notebook and pass parameters to it:
result = notebookutils.notebook.run(
    "02_silver_transform",              # Notebook name (in same workspace)
    timeout_seconds=600,                # Fail if takes > 10 minutes
    arguments={"run_date": "2024-04-20", "env": "prod"}
)
print(result)   # Returns the exit value of the child notebook

# ── Display rich output ───────────────────────────────────────────
notebookutils.display(df)               # Same as display(df) — rich table with charts
```

### 3. Passing Parameters to Notebooks (Pipeline Integration)

To run a notebook from a Pipeline with dynamic dates:

```python
# Cell 1: Mark as "parameter cell" in the UI (toggle on the cell toolbar)
# This allows the Pipeline to inject values at runtime

run_date = "2024-04-20"    # Default value (used when running manually)
env = "dev"

# Cell 2: Use the parameters throughout the notebook
print(f"Processing data for: {run_date} in environment: {env}")

input_path  = f"Files/landing/{run_date}/orders.csv"
output_table = f"bronze_orders_{env}"

df = spark.read.option("header", "true").csv(input_path)
df.write.format("delta").mode("append").saveAsTable(output_table)
```

```
In the Pipeline (Notebook Activity settings):
  → Base parameters:
       run_date = @formatDateTime(pipeline().parameters.RunDate, 'yyyy-MM-dd')
       env      = @pipeline().parameters.Environment

Fabric will inject these values, overriding the notebook defaults.
```

---

## 🏛️ Phase 3: Library Management — Installing Python Packages

### 1. Three Ways to Install Libraries

| Method | Scope | Best For |
|--------|-------|---------|
| **Inline `%pip install`** | Current session only | Quick experiments |
| **Fabric Environment** | Workspace-wide, persisted | Production pipelines |
| **Custom Docker image** | Full control (advanced) | Specific OS-level dependencies |

### 2. Inline Install (Session-Level)

```python
# Install for the current Spark session only
# (Lost when the session ends — must re-install next time)
%pip install great-expectations==0.18.7
%pip install scikit-learn==1.4.0 xgboost==2.0.3

# After install, import normally:
import great_expectations as gx
from sklearn.ensemble import RandomForestClassifier
```

> ⚠️ **Warning:** `%pip install` restarts the Python kernel. Put all pip installs in the **first cell** of your notebook.

### 3. Fabric Environments — The Production Way

A **Fabric Environment** is a saved, reusable library configuration that you attach to notebooks and pipelines.

```
Creating a Fabric Environment:
─────────────────────────────
1. Workspace → New → Environment
2. Name it: "prod-data-eng-env"
3. Go to "Public Libraries" tab
4. Search and add:
   • pandas==2.1.4
   • great-expectations==0.18.7
   • scikit-learn==1.4.0
   • delta-spark==3.0.0
5. Go to "Custom Libraries" tab
   • Upload your private .whl file if needed
6. Click "Publish" — Fabric installs and caches all libraries
7. Attach to your Lakehouse or individual notebooks:
   Notebook settings → Environment → "prod-data-eng-env"
```

```python
# Once the Environment is attached, libraries are pre-installed.
# No %pip install needed — just import:
import great_expectations as gx
from sklearn.ensemble import RandomForestClassifier

print("All libraries loaded from Environment — no re-install needed!")
```

---

## ⚡ Phase 4: Spark Pools & Performance Tuning

### 1. Starter vs Custom Spark Pools

By default, Fabric uses the **Starter Pool** — a small, pre-warmed Spark cluster that starts in seconds:

```
Starter Pool (default):
  • Driver:  4 cores, 28 GB RAM
  • Workers: Up to 4 nodes × (4 cores, 28 GB)
  • Total:   Up to 20 cores, 140 GB RAM
  • Cold start: ~15–30 seconds
  • Cost: Uses your Fabric Capacity (CUs)
```

For large workloads, create a **Custom Spark Pool**:

```
Custom Spark Pool (Workspace Settings → Spark → Spark Pools):
  • Node Size: Small (4 vCores) / Medium (8) / Large (16) / XLarge (32) / XXLarge (64)
  • Min Nodes: 1 (saves cost when idle)
  • Max Nodes: 20 (autoscale up for big jobs)
  • Autoscale: ON (recommended)
  • Dynamic Allocation: ON (Spark reallocates executors between tasks)
  • Cold start: ~2–4 minutes (larger clusters take longer)
```

### 2. Spark Configuration — Key Settings to Tune

```python
# Set Spark config at the start of your notebook for better performance:

spark.conf.set("spark.sql.shuffle.partitions", "200")
# ⬆️ Default is 200 — tune based on data size:
#    • < 10 GB data → set to 8–16
#    • 10–100 GB    → set to 100–200
#    • > 100 GB     → set to 400–800

spark.conf.set("spark.databricks.delta.optimizeWrite.enabled", "true")
# ⬆️ Fabric automatically coalesces small files when writing Delta tables

spark.conf.set("spark.databricks.delta.autoCompact.enabled", "true")
# ⬆️ Auto-merges small Delta files after writes (prevents the "small file problem")

spark.conf.set("spark.sql.adaptive.enabled", "true")
# ⬆️ AQE (Adaptive Query Execution) — Spark auto-optimizes joins and partitions at runtime
# This is ON by default in Spark 3.x — verify it's not disabled

spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# ⬆️ AQE sub-feature: merges shuffle partitions that are too small
```

### 3. Common Performance Anti-Patterns and Fixes

```python
# ❌ BAD: Collecting too much data to the driver — can OOM crash your session
all_rows = df.collect()     # Pulls ALL rows from all workers to your laptop's RAM

# ✅ GOOD: Use Spark operations to stay distributed
df.count()                  # Count stays distributed
df.show(20)                 # Show only 20 rows to driver
df.write.saveAsTable("...")  # Write distributed to Delta

# ──────────────────────────────────────────────────────────────────────

# ❌ BAD: Repeated reads (Spark re-reads the source file every time)
df1 = spark.read.csv("Files/orders.csv", header=True)
count1 = df1.count()        # Reads CSV once
count2 = df1.filter(...).count()  # Reads CSV AGAIN!

# ✅ GOOD: Cache if you'll reuse the DataFrame multiple times
df1 = spark.read.csv("Files/orders.csv", header=True).cache()
count1 = df1.count()        # Reads CSV, caches in memory
count2 = df1.filter(...).count()  # Uses cache — no re-read!
df1.unpersist()             # Free cache when done

# ──────────────────────────────────────────────────────────────────────

# ❌ BAD: UDFs (Python functions) — bypasses Spark's JVM optimizer
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

@udf(StringType())
def clean_region(region):
    return region.strip().upper()    # Slow! Serializes each row to Python

df.withColumn("region", clean_region(df.region))

# ✅ GOOD: Use built-in Spark SQL functions (all JVM-native, vectorized)
from pyspark.sql import functions as F
df.withColumn("region", F.upper(F.trim(F.col("region"))))  # 10–100x faster!
```

### 4. Delta Lake Optimization Commands

```python
# Run periodically on large Delta tables to maintain performance:

# OPTIMIZE — compacts many small Parquet files into larger, efficient ones
spark.sql("OPTIMIZE bronze_orders")

# OPTIMIZE + ZORDER — optimizes AND sorts data by frequently filtered columns
# (Makes queries with WHERE order_date = '...' or WHERE region = '...' 10x faster)
spark.sql("OPTIMIZE silver_orders ZORDER BY (order_date, region)")

# VACUUM — removes old Delta file versions (cleans up disk space)
# Default retention = 7 days (keeps 7-day time travel history)
spark.sql("VACUUM silver_orders RETAIN 168 HOURS")  # = 7 days

# DESCRIBE HISTORY — view all changes made to the table
spark.sql("DESCRIBE HISTORY silver_orders").show(truncate=False)

# TIME TRAVEL — read a previous version of the table (debugging or recovery)
df_yesterday = spark.read.format("delta").option("versionAsOf", 5).table("silver_orders")
df_before_bug = spark.read.format("delta").option("timestampAsOf", "2024-04-19").table("silver_orders")
```

### 5. Fabric V-Order — The Secret Sauce for Power BI
**V-Order** is a Microsoft-proprietary optimization for Delta Parquet files in Fabric. 
*   **What it does:** It applies advanced sorting and compression to the Parquet files so that **Power BI Direct Lake** can read them even faster.
*   **How to enable:** It is **ON by default** for all Fabric engines (Spark, SQL).
*   **The Benefit:** Reduces the file size on OneLake and increases the query speed in Power BI by up to 10×.

---

## 🎯 Phase 6: Certification & Interview Drill

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **Notebook Chaining:** How do you pass a variable from a child notebook back to a parent?
    *   **Answer:** Use `mssparkutils.notebook.exit("your_value")` in the child. The parent captures it via `result = mssparkutils.notebook.run(...)`.
*   **Spark Environments:** Where do you define an environment to be shared across 10 notebooks?
    *   **Answer:** **Workspace Settings** or create an **Environment Item** and attach it to each notebook.

### 🏢 Consultancy Scenario: "The OOM Disaster"
**Scenario:** A client's Spark job is crashing with an "Out Of Memory" (OOM) error while joining two 10GB tables.
*   **Architect Answer:** **Broadcast vs. Shuffle.**
*   **The Move:** Check if one table is small enough (<1GB). If so, use a `broadcast` join: `df1.join(broadcast(df2), "id")`. This avoids moving 10GB across the network. If both are large, increase the **Node Size** in the Custom Pool to "Large" or "XLarge" to give Spark more heap memory.

### 🚀 Startup Scenario: "The 2-Minute Cold Start"
**Scenario:** Your startup runs 100 small notebooks a day. Each one takes 2 minutes to start the Spark cluster and only 10 seconds to run the code. You're wasting money.
*   **Answer:** **Pool Sharing and Starter Pools.**
*   **The Drill:** Use the **Starter Pool**. It has a 'Pre-warmed' session feature. Also, try to consolidate multiple notebooks into one larger notebook, or use a **Pipeline** to run them sequentially in the same session to avoid multiple cold starts.

### 🏛️ FAANG Scenario: "The V-Order Portability"
**Scenario:** "If we use V-Order, can we still read the data from a different cloud tool like Databricks or Snowflake?"
*   **Answer:** **Yes, it's just Parquet.**
*   **The Drill:** V-Order is an optimization *within* the Parquet standard. Any tool that can read standard Parquet (like Databricks) can still read V-Ordered files. You get the benefit in Fabric, without losing the "Open Format" portability.

---

### 🧪 Hands-on Labs
- [spark_optimization_lab.ipynb](spark_optimization_lab.ipynb) (A lab on tuning shuffle partitions and measuring the speedup of Z-Ordering)

---

## 🔬 Phase 5: Data Science Notebooks in Fabric

### 1. MLflow Integration (Built-In)

Fabric Notebooks include MLflow for experiment tracking. Every ML run can be logged:

```python
import mlflow
import mlflow.sklearn
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error
import pandas as pd

# Set the experiment (creates it if it doesn't exist)
mlflow.set_experiment("sales_forecast_v2")

# Load training data from Lakehouse
df = spark.read.table("gold_daily_revenue").toPandas()

X = df[["day_of_week", "region_encoded", "lag_7d_revenue"]]
y = df["total_revenue"]
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# ── MLflow auto-logging ───────────────────────────────────────────
with mlflow.start_run(run_name="gbr_experiment_01"):
    mlflow.log_param("n_estimators", 200)
    mlflow.log_param("max_depth", 5)
    mlflow.log_param("learning_rate", 0.05)

    model = GradientBoostingRegressor(n_estimators=200, max_depth=5, learning_rate=0.05)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    mae = mean_absolute_error(y_test, preds)

    mlflow.log_metric("mae", mae)
    mlflow.sklearn.log_model(model, "sales_forecast_model")

    print(f"MAE: {mae:.2f}")
    print("Model logged to MLflow experiment 'sales_forecast_v2'")

# View results: Workspace → Experiments → sales_forecast_v2
```

---

### ✅ Key Takeaways

1. **No cluster management** — Fabric Spark is fully serverless. Sessions auto-start and auto-terminate.
2. **`notebookutils`** = your SDK for the Fabric platform: file ops, secrets, chaining notebooks.
3. **Parameter cells** = how you make notebooks dynamic for Pipeline invocation with runtime arguments.
4. **Fabric Environments** = the production way to install and share Python libraries. Avoid session-only `%pip install` in production.
5. **Starter Pool** = good for most workloads. Create a Custom Pool only for memory-intensive jobs (>50 GB DataFrames).
6. **AQE + auto-optimize** = leave adaptive query execution ON. Use `OPTIMIZE ZORDER` for frequently filtered Delta tables.
7. **Avoid Python UDFs** — always prefer built-in `pyspark.sql.functions` for 10–100× better performance.
8. **MLflow is built-in** — log every training run. Compare experiments in the Fabric UI without additional tooling.

---

## 🧪 Practice Exercises

### Exercise 1 — Master notebookutils (Beginner)
**Goal:** Practice the most common notebookutils operations in a real notebook.

```python
# Open "TestNotebook" in your LearningLakehouse workspace and run each cell:

# ── Cell 1: List files ─────────────────────────────────────────────────────
files = notebookutils.fs.ls("Files/landing/")
for f in files:
    print(f"Name: {f.name} | Size: {f.size} bytes | Is Dir: {f.isDir}")

# ── Cell 2: Copy a file ────────────────────────────────────────────────────
notebookutils.fs.cp(
    "Files/landing/titanic.csv",
    "Files/archive/titanic_backup.csv"
)
print("File copied to archive!")

# ── Cell 3: Verify both files exist ───────────────────────────────────────
print("Landing:", notebookutils.fs.ls("Files/landing/"))
print("Archive:", notebookutils.fs.ls("Files/archive/"))

# ── Cell 4: Move (rename) a file ──────────────────────────────────────────
notebookutils.fs.mv(
    "Files/landing/titanic.csv",
    "Files/archive/titanic_processed.csv"
)
# After this, landing/ should be empty, archive/ has both files

# ── Cell 5: Run a child notebook ──────────────────────────────────────────
# First create a notebook called "child_notebook" with:
#   print("Hello from child! Received:", run_date)
#   mssparkutils.notebook.exit("SUCCESS")

result = notebookutils.notebook.run(
    "child_notebook",
    timeout_seconds=60,
    arguments={"run_date": "2024-04-20"}
)
print(f"Child notebook returned: {result}")
```

---

### Exercise 2 — Parameterized Notebook + Pipeline (Intermediate)
**Goal:** Build a notebook that accepts runtime parameters and connect it to a pipeline.

```python
# STEP 1: Create notebook "parameterized_bronze"
# ─────────────────────────────────────────────────────────────────────────────

# Cell 1 — Mark this cell as "Parameter cell" using the toolbar toggle
run_date   = "2024-04-20"   # Default (overridden by pipeline at runtime)
source_url = "https://raw.githubusercontent.com/datasciencedojo/datasets/master/titanic.csv"

# Cell 2 — Business logic using the parameters
from pyspark.sql import functions as F
import requests

print(f"[{run_date}] Starting bronze ingestion from: {source_url}")

# For demo: read the public CSV directly
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "false") \
    .csv(source_url)

df_bronze = df \
    .withColumn("_run_date",    F.lit(run_date)) \
    .withColumn("_ingested_at", F.current_timestamp())

table_name = f"bronze_titanic_{run_date.replace('-', '_')}"
df_bronze.write.format("delta").mode("overwrite").saveAsTable(table_name)

notebookutils.notebook.exit(f"SUCCESS: {df_bronze.count()} rows → {table_name}")

# STEP 2: Create a Pipeline "ParameterizedPipeline"
# ─────────────────────────────────────────────────────────────────────────────
# Add Pipeline Parameter: "RunDate" (string, default: @formatDateTime(utcNow(),'yyyy-MM-dd'))
# Add Notebook Activity → "parameterized_bronze"
# In activity settings → Base parameters:
#   run_date = @pipeline().parameters.RunDate
#
# Run the pipeline with RunDate = "2024-01-15"
# Check Lakehouse → Tables → "bronze_titanic_2024_01_15" appears!

# STEP 3: Verify
# Run the pipeline again with RunDate = "2024-02-20"
# → bronze_titanic_2024_02_20 appears (separate table per date — date-partitioned pattern)
```

---

### Exercise 3 — Spark Performance Diagnosis (Advanced)
**Goal:** Identify and fix a slow Spark notebook using the Spark UI.

```python
# ── Step 1: Run this intentionally slow code ──────────────────────────────

import time
from pyspark.sql import functions as F

# Generate a large test DataFrame (5 million rows)
df_large = spark.range(5_000_000) \
    .withColumn("category", (F.col("id") % 50).cast("string")) \
    .withColumn("value",     (F.rand() * 1000).cast("decimal(10,2)")) \
    .withColumn("region",    F.when(F.col("id") % 3 == 0, "Asia")
                              .when(F.col("id") % 3 == 1, "Europe")
                              .otherwise("Americas"))

# BAD: Python UDF (intentionally slow for comparison)
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf

@udf(StringType())
def slow_category_label(cat):
    return f"Category-{cat}"

start = time.time()
df_bad = df_large.withColumn("label", slow_category_label(F.col("category")))
df_bad.write.format("delta").mode("overwrite").saveAsTable("perf_test_bad")
bad_time = time.time() - start
print(f"UDF duration: {bad_time:.1f}s")

# GOOD: Built-in Spark function (fast)
start = time.time()
df_good = df_large.withColumn("label", F.concat(F.lit("Category-"), F.col("category")))
df_good.write.format("delta").mode("overwrite").saveAsTable("perf_test_good")
good_time = time.time() - start
print(f"Built-in function duration: {good_time:.1f}s")

print(f"Speedup: {bad_time / good_time:.1f}×")

# ── Step 2: Check Spark UI ────────────────────────────────────────────────
# In the Fabric Notebook → click "Monitor" tab at the bottom
# Find the two jobs → compare duration and "Tasks" count
# The UDF job will show Python serialization overhead in the task timeline

# ── Step 3: Check shuffle partitions ─────────────────────────────────────
print("Default shuffle partitions:", spark.conf.get("spark.sql.shuffle.partitions"))
# For 5M rows: try setting to 16 (much less than default 200) and re-run
spark.conf.set("spark.sql.shuffle.partitions", "16")
```

---

## 💼 Common Interview Questions

**Q1: What is `notebookutils` and what are its key capabilities?**
> `notebookutils` (also `mssparkutils`) is Fabric's built-in Python SDK available inside every Fabric Notebook. Key capabilities: (1) **File system** — list, copy, move, delete files in OneLake (`notebookutils.fs.*`). (2) **Secrets** — retrieve secrets from Azure Key Vault without hardcoding credentials (`notebookutils.credentials.getSecret()`). (3) **Notebook chaining** — call another notebook and pass parameters to it, receiving its exit value (`notebookutils.notebook.run()`). (4) **Display** — rich table and chart previews (`display(df)`).

**Q2: What is a Parameter Cell and how does it work with Pipelines?**
> A Parameter Cell is a special Notebook cell (toggled via the cell toolbar) that Fabric Pipeline recognizes as the injection point for runtime arguments. When a Pipeline runs the Notebook, it overrides the default values in the parameter cell with the values passed in the Pipeline's Notebook Activity settings. This makes notebooks reusable and dynamic — the same notebook can process any date or environment without code changes.

**Q3: What is the difference between `%pip install` and a Fabric Environment?**
> `%pip install` installs a library for the **current session only** — it's lost when the session ends and must re-run on every notebook open. A **Fabric Environment** is a persisted, published library manifest (like `requirements.txt`) attached to a Lakehouse or individual notebooks. When a session starts, libraries from the Environment are pre-installed — no re-install overhead, consistent versions across all notebooks, and no risk of `%pip install` restarting the kernel mid-pipeline.

**Q4: What causes the "small file problem" in Delta Lake and how does Fabric solve it?**
> When many small writes happen (e.g., micro-batches, many appends), thousands of tiny Parquet files accumulate in the Delta table folder. Each file requires a separate read operation, so queries slow dramatically. Fabric solves this with: (1) `spark.databricks.delta.optimizeWrite.enabled=true` — coalesces small files during writes. (2) `spark.databricks.delta.autoCompact.enabled=true` — merges small files automatically after writes. (3) `OPTIMIZE table_name` — manually compacts all files into target 1 GB files. Run `OPTIMIZE ZORDER BY (date_col)` for sort-optimized reads on frequently filtered columns.

**Q5: Why should you avoid Python UDFs in PySpark and what is the alternative?**
> Python UDFs require Spark to: (1) serialize each row from JVM memory to Python process, (2) run your Python function row-by-row, (3) serialize results back to JVM. This per-row serialization overhead is 10–100× slower than native Spark functions. The alternative is `pyspark.sql.functions` — these are JVM-native, vectorized operations that process entire column batches at once with no serialization. For custom logic that truly has no built-in equivalent, use **Pandas UDFs** (`@pandas_udf`) which batch-serialize entire Series at once, reducing overhead significantly.

