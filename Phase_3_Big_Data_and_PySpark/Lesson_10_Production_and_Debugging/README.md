# Lesson 10: Production Patterns & Debugging Spark Jobs

> **Why this matters:** Knowing Spark internals is 50% of the job. The other 50% is diagnosing why jobs fail in production, reading the Spark UI like a pro, and designing pipelines that are reliable and maintainable. This lesson bridges theory and real-world engineering.

---

## 🏗️ Phase 1: Absolute Foundations

### The 5 Most Common Production Failures

| Failure | Symptom | Root Cause |
|---|---|---|
| **OOM — Driver** | `java.lang.OutOfMemoryError` in driver logs | `.collect()` on large data |
| **OOM — Executor** | Container killed by YARN/K8s | Skew, large shuffle, low memory |
| **Job Hangs at 99%** | 1 task running for hours | Data skew |
| **Stage Failed 4 Times** | `Stage failed after 4 attempts` | Repeated executor crashes |
| **Slow but Completes** | Job takes 10× longer than expected | Missing broadcast, spill, GC |

### Reading the Spark UI — Your Most Important Tool

```
http://localhost:4040  (while job is running)
http://history-server:18080  (after job completes)

Key pages:
├── Jobs      → Overall job list, failed jobs
├── Stages    → Stage duration, shuffle size
├── Tasks     → Individual task stats (find skew here!)
├── Storage   → Cached DataFrames (memory used)
├── Executors → Memory, GC time per executor
├── SQL       → Query plans, per-operator timing
└── Environment → Active Spark configs
```

---

## 🚀 Phase 2: Intermediate — OOM Debugging Decision Tree

### Step 1: Where Did the OOM Happen?

```
OOM in Driver logs?
  → "java.lang.OutOfMemoryError: Java heap space" in driver
  → Caused by: .collect(), .toPandas(), too-large broadcast
  → Fix: Remove .collect() on large data; increase spark.driver.memory

OOM in Executor / Container killed?
  → "ExecutorLostFailure" or "Container killed by YARN"
  → Go to Step 2
```

### Step 2: Check Spark UI for Executor OOM

```
Spark UI → Stages → Click the failing stage → Tasks tab

Check these columns:
┌──────────────────────────────────────────────────────────┐
│ Column            │ High Value Means                     │
├──────────────────────────────────────────────────────────┤
│ Spill (Memory)    │ Not enough execution memory          │
│ Spill (Disk)      │ Severe memory pressure               │
│ Shuffle Read Size │ Large per-task → too few partitions  │
│ GC Time           │ >10% of duration → GC problem        │
│ Duration (max vs  │ Max >> Median → data skew            │
│   median)         │                                      │
└──────────────────────────────────────────────────────────┘
```

### Step 3: Apply the Fix

```python
# Fix 1: Too few shuffle partitions (large shuffle read per task, spill)
spark.conf.set("spark.sql.shuffle.partitions", "800")  # increase

# Fix 2: Data skew (one task much slower)
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
# OR apply manual salting (see Lesson 7)

# Fix 3: Low executor memory
spark.conf.set("spark.executor.memory", "16g")
spark.conf.set("spark.executor.memoryOverhead", "4g")  # for PySpark

# Fix 4: GC pressure
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
spark.conf.set("spark.executor.extraJavaOptions", "-XX:+UseG1GC")

# Fix 5: Window function holding full partition
# Replace:
w = Window.partitionBy("country")
df.withColumn("total", sum("amount").over(w))
# With:
country_totals = df.groupBy("country").agg(sum("amount").alias("total"))
df.join(country_totals, "country")
```

---

## ⚡ Phase 3: Advanced — Diagnostic Patterns & Production Best Practices

### Diagnostic Code Snippets

```python
# 1. How many partitions does my DataFrame have?
print(f"Partitions: {df.rdd.getNumPartitions()}")

# 2. How balanced are the partitions? (detect skew)
from pyspark.sql.functions import spark_partition_id
df.withColumn("pid", spark_partition_id()) \
  .groupBy("pid") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(20)

# 3. Check estimated plan statistics
df.explain(mode="cost")

# 4. Verify predicate pushdown is happening
df.filter(col("region") == "US").explain(mode="formatted")
# Look for "PushedFilters" in the FileScan section

# 5. Check current effective config
spark.conf.get("spark.sql.shuffle.partitions")
spark.conf.get("spark.sql.adaptive.enabled")

# 6. Measure data size BEFORE expensive operations
from pyspark.sql.functions import count
print(f"Row count: {df.count():,}")
print(f"Partitions: {df.rdd.getNumPartitions()}")
# Target: count/partitions ≈ 1-10M rows per partition
```

### The Anti-Pattern Hall of Shame

```python
# ❌ ANTI-PATTERN 1: collect() inside a loop
for region in regions:
    data = df.filter(col("region") == region).collect()  # Job per iteration!
# Fix: process all regions at once
result = df.groupBy("region").agg(...)

# ❌ ANTI-PATTERN 2: count() in a log statement
logger.info(f"Processing {df.count()} records")  # Triggers a full job!
# Fix: log config, not data counts

# ❌ ANTI-PATTERN 3: Creating SparkSession inside a function called per record
def process(row):
    spark = SparkSession.builder.getOrCreate()  # DON'T do this in a task!
    ...

# ❌ ANTI-PATTERN 4: Collecting to Python, processing, creating DataFrame
rows = big_df.collect()  # Pull all to driver
result = [transform(r) for r in rows]  # Process in Python
result_df = spark.createDataFrame(result)  # Push back to cluster
# This destroys all parallelism. Do it all in Spark with map/withColumn.

# ❌ ANTI-PATTERN 5: Not coalescing before write
df.write.parquet("output/")  # 200 tiny files
# Fix:
df.coalesce(20).write.parquet("output/")  # 20 proper files
```

### Production Pipeline Template

```python
"""
Standard production Spark job structure.
"""
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_spark_session(app_name: str) -> SparkSession:
    """Create and configure SparkSession for production."""
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.parquet.enableVectorizedReader", "true") \
        .getOrCreate()


def read_input(spark: SparkSession, input_path: str):
    """Read input with explicit schema — never use inferSchema in production."""
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    schema = StructType([
        StructField("event_id", StringType(), nullable=False),
        StructField("user_id", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("event_time", TimestampType(), nullable=True),
    ])
    return spark.read.schema(schema).parquet(input_path)


def transform(df):
    """Core business logic — all transformations in one place."""
    return df \
        .filter(col("user_id").isNotNull()) \
        .filter(col("amount") > 0) \
        .withColumn("processed_at", current_timestamp()) \
        .repartition(col("user_id"))     # repartition on join key for downstream


def write_output(df, output_path: str, num_output_files: int = 50):
    """Write with optimal file count and format."""
    df.coalesce(num_output_files) \
      .write \
      .mode("overwrite") \
      .parquet(output_path)


def main():
    spark = create_spark_session("MyProductionJob")
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading input data")
    raw_df = read_input(spark, "s3://bucket/input/events/")

    logger.info("Applying transformations")
    clean_df = transform(raw_df)

    # Cache only if used multiple times
    clean_df.cache()
    logger.info("Writing to output")
    write_output(clean_df, "s3://bucket/output/events/", num_output_files=100)

    # Validate output (optional but recommended)
    output_count = spark.read.parquet("s3://bucket/output/events/").count()
    logger.info(f"Output written: {output_count:,} records")

    clean_df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
```

### Medallion Architecture in Code

```python
# Bronze: raw ingest — no transforms, preserve everything
def write_bronze(raw_df, path):
    raw_df \
        .withColumn("ingested_at", current_timestamp()) \
        .write.format("delta") \
        .mode("append") \
        .partitionBy("year", "month", "day") \
        .save(path)

# Silver: cleaned and validated
def write_silver(spark, bronze_path, silver_path):
    bronze = spark.read.format("delta").load(bronze_path)
    silver = bronze \
        .dropDuplicates(["event_id"]) \
        .filter(col("user_id").isNotNull()) \
        .filter(col("amount") > 0) \
        .withColumn("event_date", col("event_time").cast("date"))
    silver.write.format("delta") \
          .mode("overwrite") \
          .option("replaceWhere", "event_date = current_date()") \
          .partitionBy("event_date") \
          .save(silver_path)

# Gold: business metrics
def write_gold(spark, silver_path, gold_path):
    silver = spark.read.format("delta").load(silver_path)
    gold = silver \
        .groupBy("event_date", "user_id") \
        .agg(
            spark_sum("amount").alias("daily_spend"),
            count("*").alias("event_count")
        )
    gold.write.format("delta").mode("overwrite").save(gold_path)
```

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Associate Drill

- **Q: What is the Medallion Architecture?**
  > The Medallion Architecture organizes a data lakehouse into three layers: **Bronze** (raw, unmodified data), **Silver** (cleaned, validated, deduplicated data), and **Gold** (aggregated, business-ready metrics and KPIs). Each layer adds quality and reduces volume. Bronze is append-only. Silver is typically overwritten daily. Gold drives dashboards and ML.

- **Q: When would you use `.checkpoint()` vs `.cache()`?**
  > Use `.cache()` when data is reused multiple times in one job. Use `.checkpoint()` to save to HDFS when the lineage graph is extremely long (prevents stack overflow) or in Structured Streaming (guarantees recovery after failure by breaking lineage and saving state to durable storage).

### 🏢 Consultancy Scenario: "JDBC Parallel Read"

**Scenario:** A client reads a 500M-row Oracle database table into Spark. It takes 4 hours because it uses a single connection.

**Fix:** Partition the JDBC read across multiple connections:

```python
df = spark.read.jdbc(
    url="jdbc:oracle:thin:@host:1521/db",
    table="sales",
    column="sale_id",           # numeric column to partition on
    lowerBound=1,
    upperBound=500_000_000,
    numPartitions=100,          # 100 parallel DB connections
    properties={"user": "usr", "password": "pwd", "driver": "oracle.jdbc.OracleDriver"}
)
# Each connection reads: WHERE sale_id BETWEEN X AND Y
# 100x faster than single-threaded read
```

### 🏛️ FAANG Scenario: "The Pipeline Reliability Interview"

**Scenario:** Your daily ETL job fails every day due to transient S3 errors or network timeouts. How do you make it reliable?

**Answer — Multi-layer reliability:**
1. **Idempotent writes** — always write to a temp path, then atomic rename. Delta Lake MERGE handles duplicates.
2. **Checkpointing** — for streaming, checkpoint location ensures restart from last committed offset.
3. **Task retry config** — `spark.task.maxFailures=4` (default), `spark.stage.maxConsecutiveAttempts=4`.
4. **Speculative execution** — `spark.speculation=true` handles stragglers.
5. **Orchestration retry** — Airflow/Databricks Jobs retries at the job level with exponential backoff.
6. **Data quality checks** — validate row counts and null rates after each stage before promoting to next layer.

---

## ⚠️ Common Pitfalls

1. **Not setting `spark.task.maxFailures`** — the default is 4. If a task keeps failing (e.g., due to bad data in one partition), the job fails at attempt 4. Consider catching errors in the transform function for bad-data resilience.

2. **Writing without `mode()`** — default write mode is `error` (fails if path exists). Always explicitly set `mode("overwrite")` or `mode("append")`.

3. **Using `overwrite` on partitioned tables** — replaces the ENTIRE table. Use Delta's `replaceWhere` for partition-specific overwrites.

4. **Not setting `setLogLevel("WARN")`** — INFO logging fills executor logs with millions of lines, making debugging impossible.

5. **Using `show()` in production** — `show()` is for interactive development only. In production jobs, use `write()`. `show()` brings data to the driver, triggers a job, and blocks execution.

6. **Missing `spark.stop()` at the end** — in standalone/K8s mode, not stopping the session can leave executor containers running, wasting resources.

---

## 🧪 Practice Exercises

### Exercise 1 — Find the Bug (Beginner)
```python
def count_active_users(df):
    logger.info(f"Total records: {df.count()}")        # Bug?
    logger.info(f"Active: {df.filter('active=1').count()}")  # Bug?
    return df.filter("active=1")

result = count_active_users(raw_df)
final = result.groupBy("country").count()
final.show()
```
Identify all performance bugs. How many extra jobs does this trigger?

### Exercise 2 — Design the Pipeline (Intermediate)
Design a Bronze→Silver→Gold pipeline for this scenario:
- Source: Kafka topic with JSON events (user_id, product_id, amount, timestamp)
- Bronze: store raw events in Delta Lake, partitioned by date
- Silver: deduplicate by event_id, remove nulls, add `event_date` column
- Gold: daily revenue by product_id

Write the code structure (not full implementation).

### Exercise 3 — Fix the Production Job (Advanced)
This job runs daily and takes 6 hours. It should take 20 minutes. Diagnose and fix:
```python
spark = SparkSession.builder.appName("daily_agg").getOrCreate()

orders = spark.read.csv("s3://data/orders/", header=True, inferSchema=True)
customers = spark.read.csv("s3://data/customers/", header=True, inferSchema=True)

result = orders.join(customers, "customer_id") \
               .groupBy("country", "product_category") \
               .agg(sum("amount"), count("*"))

result.repartition(1).write.parquet("s3://output/daily/")
```
List at least 5 problems and fix them.

---

## 💼 Common Interview Questions

**Q1: Describe your process for debugging a Spark job that runs 10× slower than expected.**
> I start with the Spark UI: check the Jobs tab for the slowest stage, then the Tasks tab within that stage. I look for: (1) task duration skew (Max >> Median → data skew), (2) Spill columns (Memory/Disk → not enough memory per task), (3) GC Time (> 10% → GC problem), (4) Shuffle Read size (large per task → too few partitions). Then I run `df.explain(formatted)` to check if broadcasts are used, if filters are pushed down, and if there are unexpected cross-joins.

**Q2: What is the difference between `repartition` and `coalesce`?**
> `repartition(n)` performs a full shuffle and creates exactly n partitions — use to increase partitions or to redistribute data by a column (e.g., `repartition(200, "user_id")` for a join-heavy pipeline). `coalesce(n)` merges existing partitions without a shuffle — much cheaper but can only reduce partitions, not increase them. Use `coalesce` before writing to avoid small files. Use `repartition` to fix skew or increase parallelism.

**Q3: How do you ensure exactly-once processing in Spark Structured Streaming?**
> Spark Structured Streaming guarantees exactly-once semantics when: (1) the source is replayable (Kafka with offsets, S3 with file tracking), (2) checkpointing is enabled with a durable checkpoint location (HDFS, S3), and (3) the sink is idempotent or transactional (Delta Lake MERGE, database upsert). The checkpoint stores committed offsets and state, so on restart, Spark replays only uncommitted data from the source and the idempotent sink ignores duplicates.

---

## 📋 The Complete Performance Tuning Checklist

Before submitting any production Spark job, verify:

```
READING:
  [ ] Using Parquet/Delta (not CSV/JSON)
  [ ] Schema provided explicitly (no inferSchema)
  [ ] Filters applied before joins
  [ ] Only needed columns selected (projection pushdown)
  [ ] Partitioned table filtered on partition column

JOINS:
  [ ] Small tables broadcast with broadcast() hint
  [ ] AQE enabled (spark.sql.adaptive.enabled=true)
  [ ] No expressions in join keys (precompute them)
  [ ] Filters applied to both sides before join
  [ ] explain() checked for expected join strategy

MEMORY:
  [ ] spark.executor.memory sized for partition volume
  [ ] spark.executor.memoryOverhead set for PySpark (>= 2g)
  [ ] No Python row-by-row UDFs (use pandas_udf or built-ins)
  [ ] caches unpersisted after use

WRITING:
  [ ] coalesce() before write (avoid small files)
  [ ] Mode explicitly set (overwrite/append)
  [ ] Partitioned by low-cardinality columns (date, region)
  [ ] Delta Lake for tables needing updates/deletes

CONFIGURATION:
  [ ] spark.sql.shuffle.partitions set (not default 200)
  [ ] spark.sql.adaptive.enabled = true
  [ ] spark.serializer = KryoSerializer
  [ ] spark.eventLog.enabled = true (for history server)
```

[← Lesson 9: Catalyst & AQE](../Lesson_9_Catalyst_and_AQE/README.md)

---
*🎉 You've completed the Spark Deep Dive series (Lessons 5-10). You now have the internals knowledge to tackle any Spark performance problem, pass Databricks/AWS/Azure Data Engineer certifications, and architect production-grade data pipelines.*
