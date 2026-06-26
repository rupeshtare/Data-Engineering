# Lesson 5: Shuffle & Joins — The Most Expensive Operations in Spark

> **Why this matters:** Shuffle and join performance issues cause 90% of Spark job failures and slowdowns in production. Understanding these deeply separates a good data engineer from a great one.

---

## 🏗️ Phase 1: Absolute Foundations

### What is a Shuffle?

A **Shuffle** is when Spark has to **move data across the network** from one set of machines to another to complete an operation.

Think of it like this:
- You have 10 filing cabinets spread across 10 offices.
- To find all files for "Customer A", someone has to walk between every office, collect all "Customer A" files, and bring them to one desk.
- That "walking between offices" is the Shuffle.

### What triggers a Shuffle?

| Operation | Why it shuffles |
|---|---|
| `groupBy()` | All rows with same key must go to same machine |
| `join()` | Matching rows must land on the same machine |
| `distinct()` | Must see all copies of each value to deduplicate |
| `orderBy()` | Must sort globally across all partitions |
| `repartition()` | Explicitly redistributes data |

### What does NOT shuffle?

| Operation | Why no shuffle |
|---|---|
| `filter()` | Each partition filtered independently |
| `select()` | Each partition processed independently |
| `map()` / `withColumn()` | Row-by-row, no cross-partition dependency |
| `coalesce()` | Merges partitions locally, no network needed |

---

## 🚀 Phase 2: Intermediate — Shuffle Mechanics Step by Step

```
STAGE 1 (Map Side):                        STAGE 2 (Reduce Side):
┌─────────────────────┐                    ┌─────────────────────────┐
│ Partition 1         │                    │ Partition A (hash 0-33) │
│ Compute             │──────────────────► │ Fetch from ALL mappers  │
│ Hash keys           │                    │ Sort + aggregate         │
│ Write shuffle files │                    │                         │
├─────────────────────┤    NETWORK         ├─────────────────────────┤
│ Partition 2         │──────────────────► │ Partition B (hash 34-66)│
│ ...                 │                    │ Fetch from ALL mappers  │
└─────────────────────┘                    └─────────────────────────┘
       DISK I/O                                   DISK I/O + NETWORK
```

**Every shuffle costs:**
1. **Map side disk write** — shuffle output written to local disk
2. **Network transfer** — data physically moves between nodes
3. **Reduce side disk read** — data read from remote machines
4. **Sort/merge** — reduce side must sort and merge data

> 💡 This is why minimizing shuffles is the #1 Spark performance rule.

### The Critical Comparison: `reduceByKey` vs `groupByKey`

```python
# ❌ BAD: groupByKey — sends ALL values across network
rdd.groupByKey().mapValues(sum)
# Every (k, v) pair travels across the network for key k

# ✅ GOOD: reduceByKey — combines LOCALLY first, then sends summary
rdd.reduceByKey(lambda a, b: a + b)
# Each machine pre-sums its local values → sends ONE number per key
```

```
groupByKey:   (k1,1) (k1,2) (k1,3) → [network] → sum([1,2,3]) = 6
reduceByKey:  (k1,1) (k1,2) (k1,3) → local: 6  → [network] → 6
                                               ↑ 3x less data!
```

For 1 billion rows with 10 values per key: `groupByKey` sends 1B rows, `reduceByKey` sends ~100M.

### `spark.sql.shuffle.partitions` — The Most Important Config

After every shuffle, Spark creates exactly this many output partitions.

```python
# Default: 200 (almost always wrong for your job size)
spark.conf.set("spark.sql.shuffle.partitions", "200")

# Rule of thumb: target 100-200MB per partition
# If your data after shuffle is 40GB → 40000MB / 128MB = ~312 partitions
spark.conf.set("spark.sql.shuffle.partitions", "320")

# For small datasets / dev: reduce to avoid thousands of empty tasks
spark.conf.set("spark.sql.shuffle.partitions", "20")
```

---

## ⚡ Phase 3: Advanced — All 4 Join Strategies

Spark picks a join strategy automatically, but you must understand which it picked and why.

### Strategy 1: Broadcast Hash Join (BHJ) ← The Best

```
Condition: One table is smaller than spark.sql.autoBroadcastJoinThreshold (default: 10MB)

Process:
  1. Driver collects the small table entirely
  2. Broadcasts it to ALL executor nodes
  3. Each executor builds a local hash map from the small table
  4. Large table scanned locally — each row probed against local hash map
  5. NO data movement for the large table!

Cost: Zero shuffle. Fastest possible join.
```

```python
from pyspark.sql.functions import broadcast

# Automatic (if small_df < 10MB Spark detects it)
result = large_df.join(small_df, "user_id")

# Force broadcast when you KNOW a table is small:
result = large_df.join(broadcast(small_df), "user_id")

# Increase threshold (if you have memory):
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "50mb")

# Disable broadcast (useful for debugging):
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
```

### Strategy 2: Sort Merge Join (SMJ) ← Default for Large Tables

```
Process:
  1. Both tables shuffled by join key (hash partitioning)
  2. Each partition sorted by join key
  3. Two sorted streams merged (like merge sort)

Cost: 2 shuffles + 2 sorts. Expensive but memory-efficient.
Used when: Both tables are too large to broadcast.
```

```python
# Force sort merge join:
df1.hint("merge").join(df2, "id")
```

### Strategy 3: Shuffle Hash Join

```
Process:
  1. Both tables shuffled by key
  2. Smaller side loaded into a hash table in memory
  3. Larger side probed against hash table

Cost: 2 shuffles. Smaller side must fit in memory per partition.
Less common — SMJ preferred for large data.
```

### Strategy 4: Broadcast Nested Loop Join ← Avoid!

```
Process: For every row in table A, scan ALL rows in table B.
Cost: O(N × M) — catastrophic for large tables.
Triggered by: Joins with no equality condition (e.g., range joins, cross joins).
```

```python
# This triggers Broadcast Nested Loop Join — be very careful:
df1.join(df2, df1.date.between(df2.start_date, df2.end_date))
# Fix: pre-filter and reduce table sizes first
```

### How to Check Which Strategy Was Used

```python
df.explain()
# Look for:
# BroadcastHashJoin  → good, no shuffle
# SortMergeJoin      → two shuffles, check if you can broadcast one side
# BroadcastNestedLoopJoin → danger! O(N×M)
# CartesianProduct   → extreme danger! O(N×M) with no broadcast
```

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Associate Drill

- **Q: When does Spark use a Broadcast Hash Join automatically?**
  > When one side of the join is smaller than `spark.sql.autoBroadcastJoinThreshold` (default 10MB). You can also force it with the `broadcast()` hint.

- **Q: What is AQE and how does it help joins?**
  > Adaptive Query Execution (Spark 3.0+) re-optimizes joins at runtime. If a table was estimated as large but actually turns out small after shuffle, AQE can switch the join strategy from Sort Merge Join to Broadcast Hash Join on the fly.

### 🏢 Consultancy Scenario: "The Slow Join"

**Scenario:** A client's PySpark job is joining two tables and taking 4 hours. What do you do?

**Architect Answer:**
1. Run `df.explain()` — check if it's using SortMergeJoin or BroadcastHashJoin
2. Check table sizes — can either side be broadcast?
3. Check for data skew — is one partition much larger than others in Spark UI?
4. If one side is a dimension table (< 1GB), force `broadcast()` hint
5. If both are large, check if bucketing on the join key is feasible (eliminates shuffle entirely)

### 🚀 Startup Scenario: "Bucketing for Daily Joins"

**Scenario:** You join `events` and `users` tables every day on `user_id`. The join takes 30 minutes because of shuffle.

**Answer:** Use **Bucketing** — pre-partition both tables by `user_id` at write time. Then the daily join has **zero shuffle**.

```python
# Write once (setup cost):
events_df.write.bucketBy(200, "user_id").sortBy("user_id").saveAsTable("events_bucketed")
users_df.write.bucketBy(200, "user_id").sortBy("user_id").saveAsTable("users_bucketed")

# Every daily join: NO shuffle (Spark detects matching bucket counts)
spark.table("events_bucketed").join(spark.table("users_bucketed"), "user_id")
```

### 🏛️ FAANG Scenario: "Optimizing a Multi-Join Pipeline"

**Scenario:** A pipeline does: `fact JOIN dim1 JOIN dim2 JOIN dim3`. How do you optimize?

**Answer:**
1. Check sizes of dim1, dim2, dim3 — broadcast all small dimension tables
2. Apply filters on fact table BEFORE the join chain
3. Use `explain()` after each join to verify broadcast is being used
4. If dim tables are medium-sized (100MB-1GB), consider materializing filtered versions and caching

---

## ⚠️ Common Pitfalls

1. **Using `groupByKey` instead of `reduceByKey`** — sends all values over the network instead of pre-aggregating locally. Always use `reduceByKey`, `aggregateByKey`, or DataFrame's `groupBy().agg()`.

2. **Joining on expressions** — `df1.join(df2, upper(df1.name) == upper(df2.name))` prevents hash join optimization. Pre-compute the expression as a column first.

3. **Joining before filtering** — always push filters before joins. `df.filter(active=True).join(other, "id")` not `df.join(other, "id").filter(active=True)`.

4. **Too many shuffle partitions for small data** — 200 partitions on a 1GB dataset creates 200 tiny 5MB tasks with massive scheduling overhead. Set `spark.sql.shuffle.partitions` appropriately.

5. **Not using `broadcast()` hint** — Spark uses file size statistics to decide on broadcasting. If stats are stale or unavailable (e.g., data just written), it may miss the optimization. Add the hint explicitly.

---

## 🧪 Practice Exercises

### Exercise 1 — Identify the Shuffle (Beginner)
Look at this code and identify every line that triggers a shuffle:
```python
df = spark.read.parquet("sales/")
df2 = df.filter(col("amount") > 100)           # Line A
df3 = df2.select("user_id", "amount")          # Line B
df4 = df3.groupBy("user_id").sum("amount")     # Line C
df5 = df4.orderBy("sum(amount)", asc=False)    # Line D
df5.write.parquet("output/")                   # Line E
```
**Answer:** Lines C and D trigger shuffles. A, B, E do not.

### Exercise 2 — Fix the Join (Intermediate)
```python
# This join is slow. How would you fix it?
user_df = spark.read.parquet("users/")        # 500 rows, ~50KB
events_df = spark.read.parquet("events/")     # 5 billion rows, 2TB

result = events_df.join(user_df, "user_id").filter(col("country") == "US")
```

### Exercise 3 — Explain Output (Advanced)
Run `df.explain()` on a join query and paste the output. Identify:
- What join strategy did Spark choose?
- Was a broadcast used?
- How many shuffles are there?
- How would you change the plan?

---

## 💼 Common Interview Questions

**Q1: What is the difference between a narrow and a wide transformation?**
> **Narrow transformations** (map, filter, select) process each partition independently — no data movement across the network. **Wide transformations** (groupBy, join, distinct) require data from multiple partitions to be combined, triggering a **shuffle**. Wide transformations create **stage boundaries** in the execution plan.

**Q2: How would you optimize a join between a 2TB table and a 500MB table?**
> If the 500MB table can fit in executor memory, I would use the `broadcast()` hint to broadcast it to all executors, eliminating the shuffle on the larger table entirely. If 500MB is too large to broadcast, I'd consider pre-bucketing both tables on the join key so the shuffle is paid once at write time, not on every read.

**Q3: What is `spark.sql.shuffle.partitions` and what value should it be?**
> It controls the number of partitions created after a shuffle operation. The default is 200, but that's rarely right. The target is **100-200MB per partition**. So if your shuffled data is 20GB, set it to ~160. Too low → OOM and slow tasks. Too high → thousands of tiny tasks with scheduling overhead.

**Q4: What is Adaptive Query Execution (AQE)?**
> AQE (Spark 3.0+, default in 3.2+) re-optimizes the query plan *at runtime* using actual data statistics from shuffle stages. It can: (1) coalesce small shuffle partitions automatically, (2) switch from Sort Merge Join to Broadcast Hash Join if a table turns out smaller than expected, and (3) split skewed partitions in joins.

[← Lesson 4: Spark Performance](../Lesson_4_Spark_Performance/README.md) | [Next: Lesson 6: Memory Management →](../Lesson_6_Memory_Management/README.md)
