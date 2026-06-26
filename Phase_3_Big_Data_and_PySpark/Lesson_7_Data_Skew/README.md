# Lesson 7: Data Skew — The Silent Job Killer

> **Why this matters:** Data skew is the single most common reason Spark jobs "almost finish" but hang for hours. One overloaded task holds up the entire stage. Knowing how to detect and fix skew is a critical production skill.

---

## 🏗️ Phase 1: Absolute Foundations

### What is Data Skew?

**Data skew** means some partition keys have vastly more data than others.

Think of it like a restaurant: You have 10 waiters. 9 of them each have 2 tables. One waiter has 50 tables. The restaurant can't "close" until that one waiter finishes. The other 9 are idle, wasting resources.

In Spark: You have 200 tasks. 199 finish in 30 seconds. Task 200 has 100× more data — it runs for 2 hours. The entire stage is blocked waiting for it.

### Classic Skew Example

```python
# Your data has a "null" user_id for all unauthenticated users
# Events table: 1 billion rows
# user_id distribution:
#   null → 500 million rows (50%!)
#   user_001 → 1,000 rows
#   user_002 → 800 rows
#   ... (all others are tiny)

events.groupBy("user_id").count()
# Result: 1 task handles 500M rows, 199 tasks handle ~2.5M rows each
# That one null partition takes 100x longer → entire stage stalls
```

---

## 🚀 Phase 2: Intermediate — Detecting and Fixing Skew

### How to Detect Skew

**Method 1: Spark UI (visual)**
```
Spark UI → Jobs → Click the slow job → Stages → Click the slow stage → Tasks
Look at the "Duration" column:
  Healthy:  Min=28s, Median=30s, Max=32s  ← all similar
  Skewed:   Min=5s,  Median=8s,  Max=2h   ← one outlier
```

**Method 2: Analyze the data (proactive)**
```python
# Find the distribution of your groupBy key BEFORE grouping
df.groupBy("user_id").count().orderBy("count", ascending=False).show(20)

# If top 5 keys have > 10% of total rows → you have skew
total = df.count()
df.groupBy("user_id").count() \
  .withColumn("pct", col("count") / total * 100) \
  .filter(col("pct") > 5) \
  .show()
```

### Fix 1: AQE Skew Join (Spark 3.0+) — Automatic

The easiest fix. Enable it and Spark handles skew automatically during joins.

```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

# Tuning thresholds:
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# A partition is "skewed" if its size > 5× the median partition size

spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256mb")
# AND the partition is larger than 256MB
```

AQE detects skewed partitions after the shuffle and **splits** them into smaller sub-partitions, then duplicates the matching rows from the other table. All automatic.

### Fix 2: Salting — The Universal Manual Fix

For groupBy aggregations, salting spreads hot keys across multiple partitions.

```python
from pyspark.sql.functions import col, concat, lit, rand, floor, explode, array

# Step 1: Add a random "salt" suffix to the hot key
SALT_FACTOR = 10  # spread across 10 partitions

# On the large (skewed) table: add random salt
skewed_df = events_df.withColumn(
    "user_id_salted",
    concat(col("user_id"), lit("_"), (floor(rand() * SALT_FACTOR)).cast("string"))
)

# Step 2: Aggregate with the salted key
partial_agg = skewed_df.groupBy("user_id_salted") \
    .agg(spark_sum("amount").alias("partial_sum"))

# Step 3: Strip salt and re-aggregate the partial results
from pyspark.sql.functions import split
final_agg = partial_agg \
    .withColumn("user_id", split(col("user_id_salted"), "_")[0]) \
    .groupBy("user_id") \
    .agg(spark_sum("partial_sum").alias("total_amount"))
```

**For skewed joins:**
```python
# On the small (lookup) table: replicate each row for all salt values
lookup_replicated = lookup_df.withColumn(
    "salt",
    explode(array([lit(str(i)) for i in range(SALT_FACTOR)]))
).withColumn(
    "user_id_salted",
    concat(col("user_id"), lit("_"), col("salt"))
)

# On the large table: add random salt
events_salted = events_df.withColumn(
    "user_id_salted",
    concat(col("user_id"), lit("_"), (floor(rand() * SALT_FACTOR)).cast("string"))
)

# Join on salted key — hot keys now spread across 10 partitions
result = events_salted.join(lookup_replicated, "user_id_salted") \
    .drop("user_id_salted", "salt")
```

### Fix 3: Isolate Hot Keys

For joins where a small number of keys dominate (e.g., "null", "guest", "admin"):

```python
hot_keys = ["null_user", "guest", "admin"]

# Split into hot and normal DataFrames
hot_events = events_df.filter(col("user_id").isin(hot_keys))
normal_events = events_df.filter(~col("user_id").isin(hot_keys))

# Hot keys: broadcast join (tiny lookup table)
hot_result = hot_events.join(broadcast(lookup_df), "user_id")

# Normal keys: regular join
normal_result = normal_events.join(lookup_df, "user_id")

# Combine
final_result = hot_result.union(normal_result)
```

### Fix 4: `repartition` by Column

Hash partitioning distributes keys more evenly than default range partitioning:

```python
# Force hash partitioning on the join/groupBy key
df.repartition(200, "user_id").groupBy("user_id").count()
```

---

## ⚡ Phase 3: Advanced — Skew in Different Scenarios

### Skew in Aggregations

```python
# Pattern: Null key absorbs massive amounts of data
df.groupBy("country").agg(spark_sum("revenue"))
# If "country" is null for 40% of rows → one partition handles 40% of data

# Fix: Handle nulls separately
df_non_null = df.filter(col("country").isNotNull())
df_null = df.filter(col("country").isNull())

result_non_null = df_non_null.groupBy("country").agg(spark_sum("revenue"))
result_null = df_null.agg(spark_sum("revenue").alias("revenue")) \
    .withColumn("country", lit("UNKNOWN"))

final = result_non_null.union(result_null)
```

### Skew in Window Functions

```python
# Window functions hold the ENTIRE partition in memory
# If one country has 500M rows, that executor needs 500M rows in RAM → OOM

w = Window.partitionBy("country").orderBy("timestamp")
df.withColumn("running_total", spark_sum("revenue").over(w))

# Fix: If you only need a group aggregate (not ordering), use groupBy + join instead
country_totals = df.groupBy("country").agg(spark_sum("revenue").alias("total_revenue"))
df.join(country_totals, "country")  # Same result, no window skew OOM
```

### Detecting Skew with Partition Size Analysis

```python
# Count rows per partition to visualize skew
from pyspark.sql.functions import spark_partition_id

df.withColumn("partition_id", spark_partition_id()) \
  .groupBy("partition_id") \
  .count() \
  .orderBy("count", ascending=False) \
  .show(20)

# Healthy: all partitions have similar counts
# Skewed: one partition has 100× more rows than others
```

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Associate Drill

- **Q: What is data skew in Spark and how does AQE handle it?**
  > Data skew means certain partitions have far more data than others, causing some tasks to run much longer than the rest. AQE (Adaptive Query Execution) automatically detects skewed partitions after a shuffle by comparing partition sizes to the median. It then splits oversized partitions into sub-partitions and replicates the matching rows from the other join side — all transparently at runtime.

### 🏢 Consultancy Scenario: "The 99% Complete Job"

**Scenario:** A client's daily ETL always gets to 99% (199/200 tasks done) then hangs for 2 hours before completing.

**Architect Diagnosis Steps:**
1. Spark UI → Stage → Tasks tab: check if last task has 100× more data
2. Identify which column is the groupBy/join key
3. Run: `df.groupBy("key").count().orderBy("count", asc=False).show(5)`
4. If one key has millions of rows → skew confirmed

**Solutions (in order of preference):**
1. Enable AQE skew join (Spark 3.0+) — zero code change
2. Filter/remove the hot key (if it's a null or junk value)
3. Apply salting to the groupBy key
4. Isolate hot keys and process with broadcast join

### 🏛️ FAANG Scenario: "Skew at Petabyte Scale"

**Scenario:** You're joining a 100TB click stream with a 10TB user table on `user_id`. Some celebrity users have millions of events.

**Answer:**
- AQE skew join handles this automatically in Spark 3.0+
- For extra control: identify the top 1,000 high-volume user IDs, broadcast-join them separately, sort-merge-join the rest
- Consider pre-bucketing both tables on `user_id` to eliminate shuffle (one-time cost)

---

## ⚠️ Common Pitfalls

1. **Ignoring null keys** — null is a valid groupBy key in Spark. All null rows go into one partition. Always check: `df.filter(col("key").isNull()).count()`.

2. **Salting without re-aggregating** — salting splits the aggregation into partial results. You must aggregate twice: once with the salted key, once without it.

3. **Using the same SALT_FACTOR for all cases** — a factor of 10 is not always right. Choose based on skew ratio: if the largest partition is 100× the median, use a salt factor of at least 20.

4. **Forgetting to drop salt columns after join** — the salted join key is an artifact. Drop it before returning results.

5. **Applying salting to the wrong table** — in a join, you salt the large skewed table and replicate the small lookup table. Never the reverse.

---

## 🧪 Practice Exercises

### Exercise 1 — Find the Hot Key (Beginner)
Given this dataset, which key causes skew and by how much?
```python
data = [("US", 1000000), ("UK", 5000), ("DE", 3000),
        ("FR", 4000), ("null", 500000), ("CA", 2000)]
# (country, event_count)
```

### Exercise 2 — Apply Salting (Intermediate)
Given:
```python
events = spark.createDataFrame([
    ("hot_user", 100), ("hot_user", 200), ("hot_user", 150),
    ("user_a", 10), ("user_b", 20)
], ["user_id", "amount"])
```
Apply salting with SALT_FACTOR=3 to compute `sum(amount)` per `user_id`. Show all steps.

### Exercise 3 — Choose the Right Fix (Advanced)
For each scenario, which skew fix would you apply and why?

1. 500 distinct keys, top 3 keys have 60% of data, rest evenly distributed
2. Only 1 key (null) has 40% of data — it represents "unauthenticated" users
3. Spark 3.2+, joining two 1TB tables, skew discovered post-shuffle
4. Spark 2.4 (no AQE), groupBy with 5 hot keys, need exact counts

---

## 💼 Common Interview Questions

**Q1: How do you detect data skew in a Spark job?**
> In the Spark UI: go to the Stages tab, click the slow stage, and look at the Tasks tab. If the "Max" task duration is 10× or more than the "Median", you have skew. Proactively: run `df.groupBy("join_key").count().orderBy("count", asc=False).show()` to see key distribution before running expensive operations.

**Q2: What is salting and when do you use it?**
> Salting is a technique to break skewed keys across multiple partitions. You append a random number (0 to SALT_FACTOR-1) to the skewed key, so one hot key becomes N different keys across N partitions. For aggregations, you aggregate twice — first with the salt, then without. For joins, you replicate the lookup table N times with each salt value. Use it when AQE is unavailable (Spark < 3.0) or when you need fine-grained control.

**Q3: What is the difference between AQE skew join and manual salting?**
> AQE skew join is automatic — Spark detects skew after the shuffle stage and splits large partitions, requiring zero code changes. Manual salting is applied before the shuffle, requiring explicit code changes but giving more control. AQE is preferred for Spark 3.0+. Manual salting is needed for older Spark versions or for groupBy skew (AQE skew join only handles join skew).

[← Lesson 6: Memory Management](../Lesson_6_Memory_Management/README.md) | [Next: Lesson 8: File Formats & Delta Lake →](../Lesson_8_File_Formats_and_Delta_Lake/README.md)
