# Lesson 2: Delta Live Tables (DLT) — Declarative Production Pipelines

> **Goal:** Build reliable, monitored, production-grade data pipelines using Delta Live Tables — the modern way to implement the Medallion Architecture in Databricks, with built-in quality gates, automatic dependency resolution, and zero-infrastructure management.

---

## 🏗️ Phase 1: Foundations — What is DLT?

### 1. The Problem DLT Solves

Without DLT, building a Medallion pipeline requires you to manually manage:
```
❌ Job dependency order (run Bronze before Silver, Silver before Gold)
❌ Error recovery (what happens if the Silver job fails midway?)
❌ Schema evolution (new column added → manually update all downstream tables)
❌ Data quality reporting (how many rows failed? which rule?)
❌ Cluster management (what size cluster for each step?)
❌ Observability (is the pipeline healthy?)
```

**With DLT, you just write WHAT you want. Databricks manages HOW:**
```
✅ Automatic dependency resolution (Databricks builds the execution graph)
✅ Automatic error handling and retry
✅ Built-in data quality (Expectations = rules that gate data flow)
✅ Pipeline UI with row-level metrics per table
✅ Auto-scaling compute
✅ Incremental processing (only processes new data by default)
```

### 2. DLT vs. Standard Notebooks — The Mental Shift

```python
# Standard Notebook Approach (imperative — YOU manage everything):
df_bronze = spark.readStream.format("cloudFiles").load("s3://bucket/landing/")
df_bronze.writeStream.format("delta").table("bronze.orders")      # YOU trigger this

df_silver = spark.readStream.table("bronze.orders")               # YOU read bronze
df_silver_clean = df_silver.filter(...).withColumn(...)
df_silver_clean.writeStream.format("delta").table("silver.orders") # YOU trigger this

# DLT Approach (declarative — YOU describe, Databricks executes):
@dlt.table(name="bronze_orders")
def bronze_orders():
    return spark.readStream.format("cloudFiles").load("s3://bucket/landing/")
    # DLT handles: checkpointing, retries, cluster, output format

@dlt.table(name="silver_orders")
def silver_orders():
    return dlt.read_stream("bronze_orders").filter(...)   # DLT knows bronze comes first!
    # DLT handles: ordering, dependency tracking, incremental processing
```

---

## 🚀 Phase 2: Building a Complete DLT Pipeline

### 1. Auto Loader — Scalable File Ingestion (Bronze Layer)

**Auto Loader** is Databricks' built-in file discovery system. It efficiently ingests new files from cloud storage — even if millions of files appear simultaneously.

```python
# dlt_pipeline.py — The complete pipeline definition

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType

# ================================================================
# BRONZE LAYER — Raw ingestion with Auto Loader
# Rule: Perfect fidelity. No transformation. Track every file.
# ================================================================
@dlt.table(
    name="bronze_orders",
    comment="Raw orders from the e-commerce landing zone (Auto Loader)",
    table_properties={
        "delta.enableChangeDataFeed": "true",   # Enable CDC for downstream streaming
        "quality": "bronze"
    }
)
def bronze_orders():
    """
    Auto Loader ingests new files automatically.
    Uses 'cloudFiles' format with file notification for scale.
    Handles: JSON, CSV, Parquet, Avro, XML
    """
    schema = StructType([
        StructField("order_id",        StringType(),    True),
        StructField("customer_id",     StringType(),    True),
        StructField("amount",          StringType(),    True),  # String! Trust nothing from source
        StructField("region",          StringType(),    True),
        StructField("status",          StringType(),    True),
        StructField("order_timestamp", StringType(),    True),
    ])

    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("cloudFiles.schemaLocation", "/Volumes/prod/dlt_schema/orders_bronze")
            .option("cloudFiles.inferColumnTypes", "false")  # We control schema!
            .schema(schema)
            .load("/Volumes/prod/landing/orders/")
            # Metadata columns — critical for debugging!
            .withColumn("_source_file",     F.input_file_name())
            .withColumn("_ingested_at",     F.current_timestamp())
            .withColumn("_processing_date", F.current_date())
    )

# ================================================================
# BRONZE LAYER — CDC (Change Data Capture) from PostgreSQL
# ================================================================
@dlt.table(
    name="bronze_customers_cdc",
    comment="CDC stream from PostgreSQL customers table via Debezium/Kafka"
)
def bronze_customers_cdc():
    """Reads CDC events from Kafka (Debezium format)"""
    return (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", spark.conf.get("kafka.bootstrap.servers"))
            .option("subscribe", "postgres.public.customers")
            .option("startingOffsets", "latest")
            .load()
            .select(
                F.col("key").cast("string").alias("key"),
                F.from_json(F.col("value").cast("string"), cdc_schema).alias("data"),
                F.col("timestamp").alias("kafka_timestamp")
            )
            .select("key", "data.*", "kafka_timestamp")
    )
```

### 2. DLT Expectations — Data Quality Gates

Expectations are the most powerful DLT feature. They define **data quality rules** that automatically route, quarantine, or halt bad data.

```python
# Three types of expectations (choose based on business impact):

# 1. @dlt.expect() — WARN only (log the violation, keep the row)
#    Use when: violations are informational, not blocking
@dlt.table(name="silver_orders")
@dlt.expect("order_date_not_future", "order_date <= current_date()")
def silver_orders():
    return dlt.read_stream("bronze_orders")

# 2. @dlt.expect_or_drop() — QUARANTINE (drop the bad row from output)
#    Use when: bad rows must not reach downstream, but keep the pipeline running
@dlt.table(name="silver_orders")
@dlt.expect_or_drop("valid_order_id",      "order_id IS NOT NULL AND order_id > 0")
@dlt.expect_or_drop("positive_amount",     "amount > 0")
@dlt.expect_or_drop("known_region",        "region IN ('NORTH', 'SOUTH', 'EAST', 'WEST')")
def silver_orders():
    return dlt.read_stream("bronze_orders")

# 3. @dlt.expect_or_fail() — HALT the pipeline
#    Use when: violations indicate a critical source system problem
@dlt.table(name="silver_orders")
@dlt.expect_or_fail("schema_intact", "amount IS NOT NULL AND order_id IS NOT NULL")
def silver_orders():
    return dlt.read_stream("bronze_orders")
    # If ANY row violates this → entire pipeline stops + sends alert!
    # Use for: contract violations from source systems

# ====================================================================
# COMPLETE SILVER LAYER with ALL expectation types:
# ====================================================================
@dlt.table(
    name="silver_orders",
    comment="Cleaned, typed, and validated orders. Ready for business logic.",
    table_properties={"quality": "silver", "delta.enableChangeDataFeed": "true"}
)
@dlt.expect_or_fail(   "no_schema_breach",      "order_id IS NOT NULL")
@dlt.expect_or_drop(   "positive_amount",        "CAST(amount AS DOUBLE) > 0")
@dlt.expect_or_drop(   "valid_region",           "region IN ('NORTH','SOUTH','EAST','WEST','CENTRAL')")
@dlt.expect(           "reasonable_date_range",  "order_date BETWEEN '2020-01-01' AND date_add(current_date(), 1)")
def silver_orders():
    return (
        dlt.read_stream("bronze_orders")

        # TYPE CASTING — convert all strings to proper types
        .withColumn("order_id",         F.col("order_id").cast("long"))
        .withColumn("customer_id",      F.col("customer_id").cast("long"))
        .withColumn("amount",           F.col("amount").cast("decimal(12,2)"))
        .withColumn("order_date",       F.to_date(F.col("order_timestamp")))
        .withColumn("order_timestamp",  F.to_timestamp(F.col("order_timestamp")))

        # STANDARDIZE
        .withColumn("region",           F.upper(F.trim(F.col("region"))))
        .withColumn("status",           F.lower(F.trim(F.col("status"))))

        # DEDUPLICATION using watermark (streaming safe)
        .withWatermark("order_timestamp", "2 hours")
        .dropDuplicates(["order_id"])

        # SELECT only valid output columns
        .select(
            "order_id", "customer_id", "amount", "region",
            "order_date", "order_timestamp", "status", "_ingested_at"
        )
    )
```

### 3. Applying SCD Type 2 Inside DLT — `dlt.apply_changes()`

```python
# APPLY CHANGES is DLT's native SCD Type 2 (and Type 1) implementation
# It reads a CDC stream and automatically manages surrogate keys + history!

# First, define the source CDC stream:
@dlt.view(name="customers_cdc_prepared")
def customers_cdc_prepared():
    """Parse the Debezium CDC format from Kafka."""
    return (
        dlt.read_stream("bronze_customers_cdc")
        .select(
            F.col("data.op").alias("operation"),        # 'c'=create, 'u'=update, 'd'=delete
            F.col("data.after.customer_id").alias("customer_id"),
            F.col("data.after.full_name").alias("full_name"),
            F.col("data.after.city").alias("city"),
            F.col("data.after.loyalty_tier").alias("loyalty_tier"),
            F.col("data.ts_ms").alias("source_timestamp")
        )
    )

# Then apply SCD Type 2 with a single function call:
dlt.apply_changes(
    target          = "silver_dim_customer",   # The SCD Type 2 dimension table
    source          = "customers_cdc_prepared",
    keys            = ["customer_id"],          # Natural key for matching
    sequence_by     = "source_timestamp",       # Which event is "more recent"?
    stored_as_scd_type = 2                      # SCD Type 2 automatically!
    # DLT adds: __START_AT, __END_AT (effective dates)
    # DLT adds: is_current flag column
)

# The result: full SCD Type 2 dimension table, maintained automatically!
# No manual MERGE statement needed!
```

### 4. Gold Layer — Business Aggregations (Batch Mode)

```python
# ================================================================
# GOLD LAYER — Business metrics (often batch, not streaming)
# ================================================================

@dlt.table(
    name="gold_daily_revenue",
    comment="Daily revenue aggregated by region and product category",
    table_properties={"quality": "gold"},
    # Partition the Gold table for faster BI queries:
    partition_cols=["report_month"]
)
def gold_daily_revenue():
    """
    Batch aggregation from Silver.
    Uses dlt.read() (not dlt.read_STREAM()) for batch output.
    """
    return (
        dlt.read("silver_orders")           # Batch read from Silver
        .join(
            dlt.read("silver_dim_customer"),  # Join with customer dimension
            "customer_id"
        )
        .filter(F.col("status") == "completed")
        .groupBy(
            F.date_trunc("day",   "order_date").alias("report_date"),
            F.date_trunc("month", "order_date").alias("report_month"),
            "region",
            "loyalty_tier"
        )
        .agg(
            F.sum("amount").alias("total_revenue"),
            F.count("order_id").alias("total_orders"),
            F.countDistinct("customer_id").alias("unique_buyers"),
            F.avg("amount").alias("avg_order_value"),
            F.max("amount").alias("max_order_value"),
            F.sum(F.when(F.col("loyalty_tier") == "Gold", F.col("amount")).otherwise(0))
                .alias("gold_tier_revenue")
        )
    )
```

---

## 🏛️ Phase 3: Operating DLT in Production

### 1. DLT Pipeline Configuration

```json
// Databricks Workflow → Delta Live Tables → Create Pipeline
{
  "name": "Production Sales Lakehouse",
  "target": "prod",                     // Unity Catalog catalog name
  "libraries": [
    {"notebook": {"path": "/Repos/prod/pipeline/dlt_pipeline"}}
  ],
  "configuration": {
    "kafka.bootstrap.servers": "{{secrets/prod-credentials/kafka-brokers}}",
    "pipelines.trigger.interval": "5 minutes"   // Trigger interval for streaming
  },
  "clusters": [
    {
      "label": "default",
      "autoscale": {
        "min_workers": 2,
        "max_workers": 10,
        "mode": "ENHANCED"              // Enhanced autoscaling = Spark-task-aware
      }
    }
  ],
  "continuous": true,                    // true = always-on streaming; false = triggered batch
  "development": false,                  // false = production mode (no dev overrides)
  "channel": "CURRENT"                  // Databricks Runtime version channel
}
```

### 2. Monitoring DLT Pipelines

```python
# Access DLT event log programmatically (it's a Delta table!):
event_log_path = "/pipelines/<pipeline-id>/system/events"
df_events = spark.read.format("delta").load(event_log_path)

# Quality metrics per expectation:
df_quality = (
    df_events
    .filter(F.col("event_type") == "flow_progress")
    .select(
        "timestamp",
        F.col("details.flow_progress.data_quality.dropped_records").alias("dropped"),
        F.col("details.flow_progress.metrics.num_output_rows").alias("output_rows"),
        "origin.flow_name"
    )
)

# Alert if drop rate exceeds 5%:
from databricks.sdk import WorkspaceClient

drop_rate = df_quality.agg(
    (F.sum("dropped") / F.sum("output_rows")).alias("drop_rate")
).collect()[0]["drop_rate"]

if drop_rate > 0.05:
    w = WorkspaceClient()
    # Send notification...
```

---

### 3. DLT Flow vs. Table
*   **DLT Table:** A standard table definition. Every time the pipeline runs, it recomputed the table (unless it's streaming).
*   **DLT Flow:** A more advanced concept for when you need to write to the SAME table from multiple sources (e.g., Unioning 10 different APIs into one Silver table).
    -  **The Syntax:** `@dlt.append_flow(target="silver_unified")`.

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Data Engineer Professional Drill
*   **Apply Changes Into (SCD Type 2):** You must know that `dlt.apply_changes()` requires a `sequence_by` column to handle out-of-order data. If you don't provide it, DLT doesn't know which record is the "latest" version.
*   **Pipeline Channels:** Know the difference between **Current** (Stable) and **Preview** (Latest features). In production, always use `CURRENT`.

### 🛡️ DP-600 (Microsoft Fabric) Drill
*   **Fabric Dataflows Gen2:** Microsoft Fabric's equivalent to DLT. While DLT is "Code-First" (Python/SQL), Dataflows Gen2 is "UI-First" (Power Query). The core concept of **Incremental Refresh** and **Data Quality Checkouts** is the same.

### 🏢 Consultancy Scenario: "The Failed Deployment"
**Scenario:** A client deployed a DLT pipeline, but it's stuck in the "Starting" phase for 20 minutes.
*   **Architect Answer:** **Check the Event Log.**
*   **The Move:** 90% of the time, the issue is a **Cluster Configuration** error (e.g., trying to use an instance type that isn't available in that region) or a **Permissions** error on the Unity Catalog catalog. Show the client the `system/events` table to find the exact error message.

### 🚀 Startup Scenario: "DLT vs. Notebooks (Cost)"
**Scenario:** "Our startup is tiny. DLT seems more expensive than standard notebooks. Is it worth it?"
*   **Answer:** **Yes, for the 'Zero-Management' factor.**
*   **The Drill:** While DLT has a slightly higher DBU cost, it saves 10+ hours a week of manual "Sitter" work. In a startup, your time is your most expensive asset. DLT handles the retries, cleanups, and ordering automatically, allowing you to build features faster.

### 🏛️ FAANG Scenario: "The 1000 Table Dependency"
**Scenario:** "We have 1,000 tables. DLT is taking 10 minutes just to build the graph before it even starts processing data. How do we fix this?"
*   **Answer:** **Split the Pipeline.**
*   **The Drill:** A single DLT pipeline shouldn't hold your entire company's data stack. Split it into "Domain-level" pipelines (e.g., Marketing Pipeline, Finance Pipeline). Use **Unity Catalog** to share tables between them. This keeps the execution graph small and fast.

---

### 🧪 Hands-on Labs
- [dlt_advanced_lab.py](dlt_advanced_lab.py) (A complex DLT script with SCD Type 2 and multiple expectations)

---

### ✅ Key Takeaways
1. **DLT is Declarative.** Tell Databricks the structure; let it handle the plumbing.
2. **Auto Loader** is the only way to scale file ingestion at the Petabyte level.
3. **Expectations** act as your "Unit Tests" for real-time data.
4. **`apply_changes()`** is the industry standard for managing CDC and SCD history.
5. **Enhanced Autoscaling** is DLT's secret weapon for balancing cost and speed.
6. **The Event Log** is your primary source of truth for debugging and auditing.

[Next: Lesson 3: MLflow & Feature Store (MLOps in the Lakehouse) →](../Lesson_3_MLflow_and_Feature_Store/README.md)

---

## 🧪 Practice Exercises

### Exercise 1 — Your First DLT Pipeline (Beginner)
**Goal:** Create a 2-table pipeline (Bronze → Silver) using Python.

```python
# Create a new Notebook and set language to Python
# Add this code:

import dlt
from pyspark.sql import functions as F

# 1. BRONZE: Ingest raw JSON from a dummy location
@dlt.table(name="raw_events")
def raw_events():
    # Simulate some raw data
    return spark.range(100).withColumn("data", F.lit("test_event"))

# 2. SILVER: Clean and Filter
@dlt.table(name="clean_events")
@dlt.expect_or_drop("valid_id", "id > 10")
def clean_events():
    return dlt.read_stream("raw_events").withColumn("processed_at", F.current_timestamp())

# 3. Deploy:
#    Go to Workflows -> Delta Live Tables -> Create Pipeline
#    Select this notebook, specify a target catalog/schema, and click 'Start'
```

---

### Exercise 2 — SCD Type 2 Dimension Building (Intermediate)
**Goal:** Practice tracking history using `dlt.apply_changes`.

```python
import dlt
from pyspark.sql import functions as F

# Simulating a CDC stream (e.g., from Debezium)
@dlt.view(name="users_cdc")
def users_cdc():
    return spark.createDataFrame([
        (1, "Alice", "NY", 100), # Original
        (1, "Alice", "SF", 101), # Update: moved to SF
        (2, "Bob", "LA", 100)
    ], ["user_id", "user_name", "city", "ts"])

# Apply SCD Type 2 logic
dlt.apply_changes(
    target = "dim_users",
    source = "users_cdc",
    keys = ["user_id"],
    sequence_by = "ts",
    stored_as_scd_type = 2
)

# EXPECTED OUTCOME: 
# User 1 will have two rows in 'dim_users':
# One with city='NY' (is_current=false) and one with city='SF' (is_current=true).
```

---

### Exercise 3 — Data Quality Monitoring (Architect)
**Goal:** Analyze the DLT event log to calculate data "survivability" (how much data passes vs. drops).

```python
# 1. Find your pipeline ID in the DLT UI URL: .../pipelines/<PIPELINE_ID>
pipeline_id = "your-pipeline-id-here"
event_log_path = f"dbfs:/pipelines/{pipeline_id}/system/events"

# 2. Query the event log for quality metrics
df_log = spark.read.format("delta").load(event_log_path)

# Extract dropped vs output rows
quality_stats = (
    df_log
    .filter(F.col("event_type") == "flow_progress")
    .select(
        "origin.flow_name",
        F.col("details.flow_progress.data_quality.dropped_records").alias("dropped"),
        F.col("details.flow_progress.metrics.num_output_rows").alias("passed")
    )
    .groupBy("flow_name")
    .agg(F.sum("dropped").alias("total_dropped"), F.sum("passed").alias("total_passed"))
    .withColumn("survivability_pct", F.col("total_passed") / (F.col("total_passed") + F.col("total_dropped")) * 100)
)

display(quality_stats)
```

---

## 💼 Common Interview Questions

**Q1: What is the primary difference between `dlt.read()` and `dlt.read_stream()`?**
> `dlt.read_stream()` creates a streaming link; the downstream table only processes **new** data that has arrived since the last run. It uses checkpoints to stay incremental. `dlt.read()` is for batch; it re-scans the entire source table every time the pipeline runs. Use `read_stream()` for performance and `read()` for final Gold aggregations.

**Q2: How does DLT handle a record that violates an `@dlt.expect_or_fail` rule?**
> The entire pipeline execution will stop immediately. No data from that batch will be written to the target table. This is used for "Contract Violations"—when the source data is so bad that processing it would break the business logic.

**Q3: Why is `sequence_by` mandatory in `apply_changes()`?**
> In distributed systems or CDC streams, events can arrive out of order (e.g., an 'Update' arrives before a 'Create'). `sequence_by` (usually a timestamp or version number) tells DLT which record is the "Latest" version of the truth, ensuring the final table is accurate.

**Q4: What are the benefits of 'Enhanced Autoscaling' in DLT?**
> Traditional Spark autoscaling looks at CPU/Memory. **Enhanced Autoscaling** in DLT is "Spark-aware"—it looks at the task queue and predicts exactly how many nodes are needed. It shuts down nodes faster when the workload drops, often saving 20-30% on infrastructure costs compared to standard clusters.

**Q5: Can you use standard Spark code inside a DLT notebook?**
> Mostly, yes. However, you cannot use "Action" commands like `df.show()`, `df.collect()`, or `df.write()`. DLT *is* the action. You only define the dataframes (`@dlt.table`), and Databricks handles the execution and writing. Adding actions will cause the DLT initialization to fail.
