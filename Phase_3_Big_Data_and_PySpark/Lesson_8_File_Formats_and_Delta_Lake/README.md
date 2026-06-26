# Lesson 8: File Formats & Delta Lake — The Storage Layer

> **Why this matters:** Your Spark code can be perfect, but reading the wrong file format can waste 95% of your cluster's time reading data you don't need. File format choice is as critical as code optimization.

---

## 🏗️ Phase 1: Absolute Foundations

### Why File Format Matters

Imagine a 1TB CSV file. You want to compute `SELECT AVG(salary) WHERE department = 'Engineering'`.

With CSV:
- Read **all 1TB** into memory
- Deserialize **every column** of every row
- Then filter for Engineering rows
- Then compute average

With Parquet + partition pruning:
- Read **only the `salary` and `department` columns** (maybe 50GB)
- **Skip file blocks** where no Engineering employees exist (row group stats)
- Result: read maybe **2GB** instead of 1TB → 500× faster

### The Four Formats You Must Know

| Format | Layout | Compression | Schema | Best For |
|---|---|---|---|---|
| **CSV** | Row-based, text | None | External | Input only — never in production |
| **Parquet** | Columnar, binary | Snappy/Gzip | Embedded | Analytics, general ETL |
| **ORC** | Columnar, binary | ZLIB | Embedded | Hive/HBase ecosystems |
| **Delta Lake** | Parquet + log | Snappy | Embedded + versioned | Production lakehouses, ACID |

---

## 🚀 Phase 2: Intermediate — Parquet Internals

### How Parquet Stores Data

```
Parquet File:
┌──────────────────────────────────────────────┐
│ Row Group 1 (128MB by default)               │
│   ┌─────────────────┐ ┌──────────────────┐   │
│   │ Column: "salary"│ │ Column: "dept"   │   │
│   │ [80000, 95000,  │ │ ["Eng", "Eng",   │   │
│   │  72000, 110000] │ │  "HR", "Eng"]    │   │
│   │ min=72000       │ │ min="Eng"        │   │
│   │ max=110000      │ │ max="HR"         │   │
│   └─────────────────┘ └──────────────────┘   │
├──────────────────────────────────────────────┤
│ Row Group 2 (128MB)                          │
│   ...                                        │
├──────────────────────────────────────────────┤
│ Footer: Schema + Row Group Statistics        │
└──────────────────────────────────────────────┘
```

**Three ways Parquet saves work:**

1. **Column Pruning** — reads only requested columns
   ```sql
   SELECT AVG(salary) FROM employees  -- reads ONLY salary column
   -- Ignores: name, email, address, department, start_date, ...
   ```

2. **Row Group Skipping (Min/Max Stats)** — skips entire blocks
   ```sql
   WHERE salary > 200000
   -- Row Group 1 has max=110000 → skip entirely!
   -- Only read row groups where max > 200000
   ```

3. **Dictionary Encoding** — low-cardinality columns (like `department`) stored as integers
   ```
   dept column: ["Engineering"→0, "HR"→1, "Finance"→2]
   Stored as: [0, 0, 1, 0, 2, 0, ...]  ← integers are tiny
   Equality filters resolved without full decompression
   ```

```python
# Enable all Parquet optimizations:
spark.conf.set("spark.sql.parquet.filterPushdown", "true")       # push filters to scan
spark.conf.set("spark.sql.parquet.enableVectorizedReader", "true")  # batch read (1024 rows)

# Writing Parquet with optimal settings:
df.write \
    .option("parquet.block.size", str(128 * 1024 * 1024))   # 128MB row groups
    .option("parquet.page.size", str(1 * 1024 * 1024))      # 1MB pages
    .parquet("output/")
```

### The Small Files Problem

```python
# BAD: 200 shuffle partitions → 200 tiny 5MB files
df.write.parquet("output/")

# Each tiny file requires:
# - Separate S3/HDFS metadata entry
# - Separate open/close overhead on read
# - Parquet footer read per file (even if tiny)
# 10,000 small files = 10,000 footer reads before any data is read!

# GOOD: coalesce before writing
df.coalesce(20).write.parquet("output/")   # 20 files × 500MB each

# OR: control max records per file
df.write.option("maxRecordsPerFile", 1_000_000).parquet("output/")
```

### Partition Strategy for Tables

**Hive-style partitioning** (directory-based):
```python
# Write partitioned by date — creates directory structure
df.write.partitionBy("year", "month", "day").parquet("output/")

# Reads:
# output/year=2024/month=01/day=15/part-00001.parquet
# output/year=2024/month=01/day=16/part-00001.parquet

# Query with partition pruning — Spark reads ONLY matching directories
spark.read.parquet("output/").filter("year=2024 AND month=01")
# Only scans year=2024/month=01/ directories → skip all other months
```

**Partition column guidelines:**
- ✅ Good: `date`, `year`, `country`, `status` — low cardinality, commonly filtered
- ❌ Bad: `user_id`, `transaction_id` — high cardinality → millions of tiny directories

---

## ⚡ Phase 3: Advanced — Delta Lake

### Why Delta Lake Exists

Plain Parquet has critical limitations:
- **No ACID** — two writers simultaneously = corrupted table
- **No upserts** — can't update a single row
- **No deletes** — GDPR "right to be forgotten" is impossible
- **No time travel** — can't query "what did this table look like yesterday?"
- **No schema enforcement** — bad data silently corrupts your table

Delta Lake solves all of these by adding a **transaction log** on top of Parquet.

### Delta Lake Architecture

```
Delta Table on S3/HDFS:
  my_table/
    ├── _delta_log/                            ← The transaction log
    │   ├── 00000000000000000000.json          ← Commit 0: initial write
    │   ├── 00000000000000000001.json          ← Commit 1: append
    │   ├── 00000000000000000002.json          ← Commit 2: update/delete
    │   └── 00000000000000000010.checkpoint.parquet  ← Checkpoint every 10
    ├── part-00001-abc.parquet                 ← Data files (Parquet)
    ├── part-00002-def.parquet
    └── part-00003-ghi.parquet
```

Each commit JSON records: which files were **added** and which were **removed**.

### Core Delta Lake Operations

```python
# Write
df.write.format("delta").mode("overwrite").save("s3://lake/my_table/")
df.write.format("delta").mode("append").save("s3://lake/my_table/")

# Read
df = spark.read.format("delta").load("s3://lake/my_table/")

# Using Delta Table object
from delta.tables import DeltaTable
delta_table = DeltaTable.forPath(spark, "s3://lake/my_table/")

# MERGE (Upsert) — impossible with plain Parquet
delta_table.alias("target").merge(
    source_df.alias("source"),
    "target.id = source.id"
).whenMatchedUpdateAll()       # update existing rows
 .whenNotMatchedInsertAll()    # insert new rows
 .execute()

# DELETE (for GDPR compliance)
delta_table.delete(col("user_id") == "user_to_delete")

# UPDATE
delta_table.update(
    condition=col("status") == "pending",
    set={"status": lit("processed"), "updated_at": lit("2024-01-15")}
)

# TIME TRAVEL — query historical versions
spark.read.format("delta").option("versionAsOf", 3).load("s3://lake/my_table/")
spark.read.format("delta").option("timestampAsOf", "2024-01-01").load(...)

# See history
delta_table.history().show()
```

### Delta OPTIMIZE — Critical for Production Tables

Delta tables accumulate small files from streaming writes and frequent appends. `OPTIMIZE` compacts them:

```python
# Compact small files (run daily or weekly)
delta_table.optimize().executeCompaction()

# Z-ORDER: co-locate data with same values in same files
# Dramatically improves query performance for multi-column filters
delta_table.optimize().executeZOrderBy("user_id", "event_date")
# Z-ordering: queries filtering on user_id OR event_date skip far more files

# VACUUM: remove old data files no longer needed (be careful — breaks time travel!)
delta_table.vacuum(retentionHours=168)   # keep 7 days of history, delete older
```

### Schema Evolution in Delta

```python
# Strict mode (default): fail if source has new columns
df.write.format("delta").mode("append").save("s3://lake/my_table/")
# Error if df has new columns not in the existing schema

# Allow schema evolution: merge new columns automatically
df.write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .save("s3://lake/my_table/")
    
# Force overwrite schema: replace schema entirely
df.write.format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .save("s3://lake/my_table/")
```

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Associate Drill

- **Q: What is the difference between Parquet and Delta Lake?**
  > Parquet is a columnar file format — it stores data efficiently but has no ACID guarantees, no transaction log, and no built-in support for updates/deletes. Delta Lake wraps Parquet with a transaction log (`_delta_log`), enabling ACID transactions, upserts (MERGE), deletes, time travel, and schema enforcement. In practice, Delta Lake IS Parquet files — just with extra metadata for transaction management.

- **Q: What is Z-ORDER and when should you use it?**
  > Z-ORDER is a data layout optimization that co-locates rows with similar values in the same Parquet files. It works best when you frequently filter on multiple columns (e.g., `WHERE user_id = 'X' AND event_date = 'Y'`). Z-ORDER maximizes the number of files that can be skipped based on Parquet's min/max statistics. Use it on your most common filter columns — typically 2-4 columns max.

### 🏢 Consultancy Scenario: "GDPR Delete Request"

**Scenario:** A client needs to delete all data for a specific user (GDPR right to erasure). Their data is in Parquet files.

**Answer:**
- Plain Parquet: **impossible** without rewriting entire partitions.
- Delta Lake: one line — `delta_table.delete(col("user_id") == "deleted_user_id")`
- Delta records the deletion in the transaction log. Files containing the row are logically "removed" from the table. After `VACUUM`, they are physically deleted.

### 🏛️ FAANG Scenario: "Concurrent Writes to the Same Table"

**Scenario:** You have 50 microservices each appending events to a shared table simultaneously. With plain Parquet, files get corrupted. How do you handle this?

**Answer:** Delta Lake uses **optimistic concurrency control**:
1. Each writer reads the current table version
2. Each writes its data to new files
3. On commit, Delta atomically checks if another writer committed first
4. If conflict: one writer wins, the other automatically retries with the latest version
5. This ensures ACID — no corruption, no lost writes

---

## ⚠️ Common Pitfalls

1. **Reading CSV with `inferSchema=True`** — Spark reads the entire file twice (once for schema, once for data). Always provide the schema explicitly with `spark.read.schema(schema).csv(...)`.

2. **Too many partition columns** — `partitionBy("user_id")` on a 100M user dataset creates 100M directories. Only partition on low-cardinality columns (date, country, status).

3. **Not running `OPTIMIZE` on Delta tables** — streaming jobs create thousands of tiny files (1KB each). Without periodic `OPTIMIZE`, reads become extremely slow due to per-file overhead.

4. **Running `VACUUM` with too short retention** — if you need time travel or concurrent jobs reading old versions, don't vacuum those versions. Default 7 days (`retentionHours=168`) is the minimum Delta enforces.

5. **Using `overwrite` mode on partitioned tables** — `mode("overwrite")` on a partitioned write replaces the ENTIRE table, not just the matching partitions. Use Delta Lake's `replaceWhere` option instead.

```python
# BAD: wipes entire table
df.write.mode("overwrite").partitionBy("date").parquet("table/")

# GOOD with Delta: only replaces specified partition
df.write.format("delta") \
    .option("replaceWhere", "date = '2024-01-15'") \
    .mode("overwrite") \
    .save("table/")
```

---

## 🧪 Practice Exercises

### Exercise 1 — Format Selection (Beginner)
Which format would you use for each scenario?
1. Daily batch ETL results consumed by Tableau/Power BI
2. Raw event logs from Kafka (append-only, never updated)
3. Customer profile table that needs GDPR deletes
4. Temporary intermediate results in a Spark pipeline
5. Data shared between Spark and Apache Hive

### Exercise 2 — Parquet Predicate Pushdown (Intermediate)
```python
# Will Spark use predicate pushdown for these filters?
df = spark.read.parquet("sales/")

# Filter 1:
df.filter(col("amount") > 1000)

# Filter 2:
df.filter(upper(col("country")) == "US")  # expression on column

# Filter 3:
df.filter(col("sale_date") > "2024-01-01")
```
Which filters can be pushed into the Parquet scan, and why?

### Exercise 3 — Delta MERGE (Advanced)
Write a Delta MERGE statement to:
- Update `status` to 'shipped' and `updated_at` to current timestamp for orders that exist in both source and target
- Insert new orders from source that don't exist in target
- Delete orders from target where `status = 'cancelled'` AND they don't exist in source

---

## 💼 Common Interview Questions

**Q1: Why is Parquet faster than CSV for analytical queries?**
> Parquet is columnar — it stores all values of a column together, so queries that access only a few columns skip reading the rest entirely (column pruning). Parquet also stores min/max statistics per row group, letting Spark skip entire blocks that can't satisfy a WHERE filter (row group skipping). CSV is row-based text — every query reads 100% of the data and parses every field as strings.

**Q2: What is Delta Lake's transaction log and why does it matter?**
> The `_delta_log` directory stores JSON files, each representing one committed transaction. Each JSON records which Parquet files were added and removed. This log is the source of truth for the table's current state. It enables ACID (multiple writers commit atomically without corruption), time travel (query older log versions), and audit history. Without it, you have plain Parquet with no transactional guarantees.

**Q3: What is Z-ORDER and how does it improve query performance?**
> Z-ORDER is a space-filling curve that maps multi-dimensional data (multiple columns) to a 1D ordering. When you Z-ORDER a table by `user_id` and `event_date`, rows with similar values for both columns end up in the same Parquet files. Since Parquet stores min/max stats per file, queries filtering on either column can skip far more files than with random ordering. The benefit increases with the number and selectivity of your filter columns.

[← Lesson 7: Data Skew](../Lesson_7_Data_Skew/README.md) | [Next: Lesson 9: Catalyst Optimizer & AQE →](../Lesson_9_Catalyst_and_AQE/README.md)
