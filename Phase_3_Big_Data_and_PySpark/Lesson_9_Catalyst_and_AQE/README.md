# Lesson 9: Catalyst Optimizer & Adaptive Query Execution (AQE)

> **Why this matters:** The Catalyst optimizer is what makes DataFrames 10-100× faster than raw RDD code. Understanding it explains WHY Spark rewrites your code, and AQE (Spark 3.0+) explains how Spark gets smarter as a job runs.

---

## 🏗️ Phase 1: Absolute Foundations

### What is the Catalyst Optimizer?

When you write DataFrame code, you're describing **what** you want — not **how** to do it. Catalyst figures out the most efficient **how**.

```python
# You write:
spark.read.parquet("huge_table/") \
    .select("id", "name", "city") \
    .filter(col("city") == "NYC")

# Catalyst rewrites this to:
# 1. Read ONLY "id", "name", "city" columns from Parquet (column pruning)
# 2. Apply city == "NYC" filter AT FILE SCAN TIME (predicate pushdown)
# 3. Skip entire Parquet row groups where city max < "NYC" (statistics)
# Result: reads ~1% of the original data
```

### The 4 Optimization Phases

```
Your DataFrame code
        ↓
[1] Unresolved Logical Plan  (what you wrote, column names not verified)
        ↓  Analysis: resolve column names, check schema
[2] Resolved Logical Plan    (all columns verified, types checked)
        ↓  Logical Optimization: apply rules
[3] Optimized Logical Plan   (filters pushed down, constants folded, etc.)
        ↓  Physical Planning: choose algorithms
[4] Physical Plan(s)         (join strategy selected, sort order decided)
        ↓  Code Generation: Tungsten compiles to JVM bytecode
[5] Executed JVM Code        (runs on executors)
```

### How to See the Plans

```python
# See all 4 plans
df.explain(mode="extended")

# Most readable (Spark 3+)
df.explain(mode="formatted")

# See estimated costs (useful for join strategy decisions)
df.explain(mode="cost")

# See generated JVM code (advanced)
df.explain(mode="codegen")
```

---

## 🚀 Phase 2: Intermediate — Key Optimization Rules

### Rule 1: Predicate Pushdown

Move WHERE filters as early as possible — ideally into the file scan.

```python
# You write:
df = spark.read.parquet("sales/")
df_filtered = df.select("amount", "region").filter(col("region") == "APAC")

# Catalyst rewrites to (check explain() output):
# FileScan parquet [amount, region]
#   PushedFilters: [EqualTo(region, APAC), IsNotNull(region)]
# → Filter applied during file read, not after!
```

**When pushdown DOESN'T work:**
```python
# Python UDF — Catalyst can't push this into the file scan
@udf("boolean")
def is_apac(region):
    return region == "APAC"

df.filter(is_apac(col("region")))  # No pushdown → reads all data!
# Fix: use col("region") == "APAC" instead (built-in expression)
```

### Rule 2: Projection Pushdown

Only read the columns you actually need.

```python
df.select("user_id", "amount").groupBy("user_id").sum("amount")
# Catalyst reads ONLY user_id and amount columns from Parquet
# Even if the table has 200 columns
```

### Rule 3: Constant Folding

Spark simplifies constant expressions at planning time, not at runtime.

```python
df.filter(col("amount") > 100 + 50)   # Catalyst computes 150 at plan time
df.filter(lit(1) == lit(1))            # Always true → remove filter entirely
df.filter(lit(1) == lit(2))            # Always false → return empty dataset early
```

### Rule 4: Join Reordering

Catalyst puts smaller tables first in joins to minimize shuffle volume.

```python
# You write:
huge_table.join(medium_table, "id").join(tiny_lookup, "type")

# Catalyst may reorder to:
# tiny_lookup broadcast → medium_table (broadcast join, no shuffle) 
# then join with huge_table
```

### Rule 5: Null Propagation

Spark short-circuits null comparisons:

```python
df.filter(col("name") == "Alice")
# Catalyst adds: IsNotNull(name) AND EqualTo(name, Alice)
# Null rows excluded early without computing the equality
```

---

## ⚡ Phase 3: Advanced — Adaptive Query Execution (AQE)

### What is AQE?

Static query optimization (Catalyst) plans based on **statistics that may be stale or estimated**. AQE re-plans based on **actual runtime data**.

```
Static planning (Catalyst):           AQE (Runtime):
"I estimate table A = 100MB"          Stage 1 runs → actual size: 5MB!
→ plan SortMergeJoin                  → switch to BroadcastHashJoin
                                      (no shuffle needed!)
```

AQE is enabled by default in Spark 3.2+:
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
```

### AQE Feature 1: Coalescing Shuffle Partitions

The problem with `spark.sql.shuffle.partitions = 200`: for a small result, you get 200 mostly-empty partitions with 200 tiny tasks.

```python
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128mb")

# Before AQE: 200 partitions, each 2MB (tiny!)
# After AQE:  Spark sees actual sizes → merges into 20 partitions of 20MB each
# Fewer tasks, less scheduling overhead
```

### AQE Feature 2: Dynamic Join Strategy Switching

```python
# At plan time: table B estimated at 500MB → plan SortMergeJoin (2 shuffles)
# At runtime: after Stage 1, table B is actually 8MB!
# AQE switches: BroadcastHashJoin → Stage 2 runs with ZERO shuffle!

spark.conf.set("spark.sql.adaptive.enabled", "true")
# That's it — AQE handles the rest automatically
```

### AQE Feature 3: Skew Join Optimization

```python
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
# Partition is "skewed" if it's 5× larger than the median

# AQE detects after the shuffle:
# Partition 7: 10GB (skewed!)
# Median: 128MB

# AQE splits partition 7 into sub-partitions
# and replicates matching rows from the other table
# Result: task 7 no longer hangs for hours
```

### Dynamic Partition Pruning (DPP) — Related to AQE

DPP uses filter results from a dimension table to prune partitions of a fact table at runtime:

```python
# sales table is partitioned by date_id
# dim_date table is small (broadcast)

spark.sql("""
    SELECT s.amount, d.quarter
    FROM sales s JOIN dim_date d ON s.date_id = d.date_id
    WHERE d.year = 2024 AND d.quarter = 'Q4'
""")

# DPP does this:
# 1. Evaluate WHERE d.year=2024 AND d.quarter='Q4' on dim_date
#    → Gets set of date_ids: {20241001, 20241002, ..., 20241231}
# 2. Uses those IDs to prune sales partitions BEFORE scanning
#    → Only reads sales/date_id=20241001/ through sales/date_id=20241231/
#    → Skips all other years' data entirely!
```

```python
spark.conf.set("spark.sql.optimizer.dynamicPartitionPruning.enabled", "true")  # default ON
```

### Tungsten — The Execution Engine

Tungsten is what actually runs your query after Catalyst plans it:

1. **Whole-Stage Code Generation (WSCG)**: Multiple operators fused into one JVM function
   - Instead of: `filter → project → aggregate` (3 virtual function calls per row)
   - Generates: one tight loop that does all 3 in one pass
   - Look for `*(1)` and `*(2)` in `explain()` output — these are WSCG stages

2. **Vectorized Execution**: Processes 1,024 rows at a time (columnar batch)
   - More cache-friendly than row-by-row
   - Enables SIMD CPU instructions

3. **Off-heap binary storage**: No JVM object overhead, no GC

```python
# Check if WSCG is active for your query:
df.explain(mode="codegen")
# Look for: "WholeStageCodegen" sections
```

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Associate Drill

- **Q: What are the 4 phases of Catalyst optimization?**
  > (1) Analysis — resolve column names and validate schema, (2) Logical Optimization — apply rules like predicate pushdown, constant folding, (3) Physical Planning — choose algorithms like which join strategy to use, (4) Code Generation — Tungsten compiles to optimized JVM bytecode.

- **Q: What does AQE do that static Catalyst cannot?**
  > Catalyst plans based on estimated statistics (which may be wrong). AQE re-optimizes at runtime using actual data measurements collected between shuffle stages. AQE can: (1) coalesce small post-shuffle partitions, (2) switch from Sort Merge Join to Broadcast Hash Join when a table is smaller than expected, (3) split skewed join partitions automatically.

### 🏢 Consultancy Scenario: "Optimizer Not Pushing Filters"

**Scenario:** A client's query reads 100GB but only returns 1,000 rows. The filter should eliminate 99% of data. Why isn't the optimizer helping?

**Diagnosis checklist:**
1. Run `df.explain()` — check `PushedFilters` section in FileScan
2. If filter NOT in PushedFilters → it's not being pushed
3. Common reasons:
   - Filter uses a Python UDF (can't be pushed)
   - Filter on a derived column (`upper(col("name"))`) not on the raw column
   - Reading from a non-Parquet format (CSV has no pushdown)
   - Column statistics outdated (run `ANALYZE TABLE`)

### 🚀 Startup Scenario: "Why Didn't Spark Broadcast?"

**Scenario:** You have a 500KB lookup table. Spark chose Sort Merge Join instead of Broadcast. Why?

**Possible reasons:**
1. **Stats unavailable** — Spark doesn't know the table size (recently written, no stats)
2. **Threshold too low** — 500KB > `spark.sql.autoBroadcastJoinThreshold` if set to `-1`
3. **Disabled by hint** — a `.hint("merge")` somewhere overrides broadcast

**Fix:**
```python
# Explicit broadcast hint — always works regardless of stats
large_df.join(broadcast(small_df), "key")

# Or update table statistics:
spark.sql("ANALYZE TABLE my_table COMPUTE STATISTICS")
```

---

## ⚠️ Common Pitfalls

1. **Python UDFs block all optimizations** — the moment you use a Python UDF in a filter, Catalyst can't push it into the file scan. Use built-in functions (`col`, `lit`, `when`, etc.) or Pandas UDFs.

2. **Trusting `explain()` without running** — Catalyst's `explain()` shows the planned execution. With AQE, the actual execution may differ. Check the Spark UI's "SQL" tab for the actual runtime plan.

3. **Not analyzing tables for statistics** — Catalyst uses statistics (row counts, column min/max/NDV) to make decisions. Without stats, it guesses. Run `ANALYZE TABLE` after major data loads.

4. **Disabling AQE** — some teams disable AQE thinking it's experimental. In Spark 3.2+ it's production-stable and provides free optimization. Keep it on.

5. **Join hints override AQE** — if you add a `.hint("merge")`, AQE cannot switch it to broadcast at runtime. Only add hints when you're sure about the data sizes.

---

## 🧪 Practice Exercises

### Exercise 1 — Read the Explain Plan (Beginner)
Given this `explain()` output, answer the questions:
```
== Physical Plan ==
*(2) HashAggregate(keys=[city#12], functions=[count(1)])
+- Exchange hashpartitioning(city#12, 200), ENSURE_REQUIREMENTS
   +- *(1) HashAggregate(keys=[city#12], functions=[partial_count(1)])
      +- *(1) Filter (isnotnull(age#8) AND (age#8 > 25))
         +- *(1) FileScan parquet [age#8,city#12]
                PushedFilters: [IsNotNull(age), GreaterThan(age,25)]
```
1. How many shuffles are there?
2. How many columns are read from Parquet?
3. Was the filter pushed to the scan?
4. What does `*(1)` mean?

### Exercise 2 — Force AQE Behavior (Intermediate)
```python
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "1000")

# This query produces only 500KB of shuffle output
df.groupBy("country").count().write.parquet("output/")
```
Without AQE: how many output files? With AQE: approximately how many? Why?

### Exercise 3 — Diagnose Filter Not Pushed (Advanced)
```python
# This query reads all 100GB despite the WHERE clause. Why?
@udf("boolean")
def is_active(status):
    return status == "active"

spark.read.parquet("users/") \
    .filter(is_active(col("status"))) \
    .select("user_id", "email") \
    .show()
```
Fix this code to enable predicate pushdown.

---

## 💼 Common Interview Questions

**Q1: How does the Catalyst optimizer differ from database query optimizers like PostgreSQL?**
> Catalyst is rule-based + cost-based (with AQE adding runtime feedback). It applies a set of transformation rules (predicate pushdown, constant folding) and then uses cost models to choose physical algorithms (which join strategy). Traditional database optimizers do similar things but in a single-node context. Catalyst is designed for distributed execution, where the cost of data movement (shuffle) is a primary concern alongside compute cost.

**Q2: What is Whole-Stage Code Generation and why does it matter?**
> WSCG (Tungsten feature) fuses multiple operators into a single JVM function that processes data in a tight loop. Without WSCG, each operator (filter, project, aggregate) makes separate virtual function calls per row — expensive at billions of rows. With WSCG, the chain of operators becomes one native-ish JVM loop with no overhead between operators. It also enables CPU cache-friendly processing and vectorized execution.

**Q3: Can AQE change a Sort Merge Join to a Broadcast Hash Join at runtime?**
> Yes — this is one of AQE's three core features. If Catalyst planned a Sort Merge Join based on estimated table sizes, but after Stage 1 the actual shuffled data is smaller than `spark.sql.autoBroadcastJoinThreshold`, AQE inserts a broadcast and switches to Broadcast Hash Join for Stage 2. This eliminates one shuffle and can dramatically speed up the join.

[← Lesson 8: File Formats & Delta Lake](../Lesson_8_File_Formats_and_Delta_Lake/README.md) | [Next: Lesson 10: Production Patterns & Debugging →](../Lesson_10_Production_and_Debugging/README.md)
