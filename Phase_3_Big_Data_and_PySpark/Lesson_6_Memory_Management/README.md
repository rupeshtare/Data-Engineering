# Lesson 6: Memory Management — How Spark Uses RAM

> **Why this matters:** Every OOM (Out of Memory) error, every unexplained slowdown, every GC pause traces back to how Spark manages memory. This is the single most important concept for debugging production jobs.

---

## 🏗️ Phase 1: Absolute Foundations

### The Simple Mental Model

When you run a Spark job, every Executor (worker) gets a chunk of RAM. Spark divides that RAM into buckets with specific jobs:

```
Your 8GB Executor RAM
├── 300MB → Reserved (Spark internals — you can't touch this)
└── 7.7GB → Usable
    ├── 4.6GB → Spark Unified Pool (60% of usable)
    │   ├── Execution Memory  ← shuffle, sort, aggregations
    │   └── Storage Memory    ← cached DataFrames, RDDs
    └── 3.1GB → User Memory (40%) ← your Python objects, UDFs
```

The key insight: **Execution and Storage share one pool and can borrow from each other.**

If a shuffle needs more memory, it can push cached data out. If cached data is critical, it holds its space until execution needs it.

### When does Spark run out of memory?

1. **Shuffle too large** — too much data per partition, not enough execution memory
2. **Too much cached data** — storage fills up, nothing left for execution
3. **Data skew** — one partition has 100x more data than others
4. **Python overhead** — PySpark's Python processes use memory outside the JVM

---

## 🚀 Phase 2: Intermediate — Memory Architecture Deep Dive

### The Unified Memory Manager (Spark 1.6+)

```python
# Key configs:
spark.executor.memory        = "8g"    # Total JVM heap
spark.memory.fraction        = 0.6    # 60% → Spark unified pool
spark.memory.storageFraction = 0.5    # 50% of unified pool → storage "soft limit"

# Calculated values:
# Reserved          = 300MB (fixed)
# Usable            = 8192MB - 300MB = 7892MB
# Unified Pool      = 7892MB × 0.6  = 4735MB
# Storage soft limit = 4735MB × 0.5 = 2367MB
# User Memory       = 7892MB × 0.4  = 3156MB
```

**The borrow mechanism:**
- Execution can evict cached data (drop from storage) if it needs more memory
- Storage can use execution memory up to the soft limit
- Neither can exceed the unified pool total

### Memory Overhead — The Hidden Extra

```
Total container memory = spark.executor.memory + spark.executor.memoryOverhead
```

`memoryOverhead` covers what lives OUTSIDE the JVM heap:
- JVM internal structures (class metadata, thread stacks)
- Python worker processes (critical for PySpark!)
- Off-heap data (if enabled)

```python
# Default: max(executor_memory × 0.1, 384MB)
# For PySpark with heavy UDFs, increase significantly:
spark.conf.set("spark.executor.memoryOverhead", "2g")

# YARN/K8s will kill your container if:
# executor.memory + memoryOverhead > container limit
```

### Off-Heap Memory — Escaping the GC

```python
# Enable off-heap for large-scale caching (avoids JVM GC)
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")
```

Off-heap memory:
- Not managed by Java Garbage Collector → no GC pauses
- Tungsten engine uses this for binary data processing
- Useful when your cache is huge and GC pauses are killing performance

### Memory Spill — The Safety Valve

When Spark runs out of execution memory during a shuffle or sort:
1. It writes the excess data to local disk (**spill to disk**)
2. Processing continues — it does NOT crash
3. But disk I/O makes the task much slower

**How to spot spill in Spark UI:**

```
Go to: Spark UI → Jobs → Click a Stage → Tasks tab
Look at columns: "Spill (Memory)" and "Spill (Disk)"
If those columns have non-zero values → you are spilling
```

**Causes of spill:**
- Too few partitions → too much data per partition
- Not enough executor memory
- Data skew — one key's partition is enormous

---

## ⚡ Phase 3: Advanced — GC Tuning and Serialization

### Java Garbage Collection — Why It Kills Spark

JVM GC runs when heap memory is nearly full. During GC, the JVM **pauses all threads** (Stop-the-World). For Spark:
- Task progress stops completely
- Network timeouts can occur (heartbeat missed)
- Long GC looks like a hung task

**Diagnosing GC problems:**
```
Spark UI → Executors tab → "GC Time" column
If GC Time / Task Duration > 10% → you have a GC problem

Symptoms:
- Tasks are slow but "shuffle read" shows little data
- CPU usage low despite active tasks
- Executor logs show: "Full GC" events
```

**Fixes:**

```python
# 1. Use Kryo serialization (much smaller object footprint than Java default)
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# 2. Use G1GC (better for large heaps, common in Spark deployments)
# Add to executor JVM options:
spark.conf.set("spark.executor.extraJavaOptions",
    "-XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:InitiatingHeapOccupancyPercent=35")

# 3. Enable off-heap to reduce GC pressure on cached data
spark.conf.set("spark.memory.offHeap.enabled", "true")
spark.conf.set("spark.memory.offHeap.size", "4g")
```

### Kryo Serialization — Why It Matters

When Spark moves data around (spill to disk, shuffle, cache in serialized form), it must serialize objects.

| Serializer | Size | Speed | Notes |
|---|---|---|---|
| Java (default) | Large | Slow | Works with any class |
| Kryo | 2-10x smaller | 3-5x faster | Requires class registration for best perf |

```python
spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

# For custom classes, register them (optional but faster):
from pyspark import SparkConf
conf = SparkConf()
conf.set("spark.kryo.classesToRegister", "com.mypackage.MyClass")
```

### Storage Levels — Choosing How to Cache

```python
from pyspark import StorageLevel

df.persist(StorageLevel.MEMORY_ONLY)          # RAM only, no serialization → fastest read
df.persist(StorageLevel.MEMORY_AND_DISK)      # RAM first, spill to disk → safe default
df.persist(StorageLevel.MEMORY_ONLY_SER)      # Serialized in RAM → 2-5x less memory
df.persist(StorageLevel.DISK_ONLY)            # Disk only → for expensive-to-recompute data
df.persist(StorageLevel.OFF_HEAP)             # Off-heap memory → no GC pressure

# .cache() = MEMORY_AND_DISK (the safe default)
```

**Decision guide:**
```
Data used many times in hot loop? → MEMORY_ONLY (fastest access)
Large data that might not fit in RAM? → MEMORY_AND_DISK (safe)
Memory is precious, can tolerate slower reads? → MEMORY_ONLY_SER
GC pauses hurting performance? → OFF_HEAP
Very expensive recomputation? → DISK_ONLY
```

**Always unpersist when done:**
```python
df.cache()
df.count()          # triggers caching
# ... use df many times ...
df.unpersist()      # ← never forget this! releases memory for other jobs
```

---

## 🎯 Phase 4: Certification & Interview Drill

### 🛡️ Databricks Associate Drill

- **Q: What is the difference between `cache()` and `persist()`?**
  > `cache()` is shorthand for `persist(StorageLevel.MEMORY_AND_DISK)`. `persist()` lets you specify the storage level explicitly. For most use cases, `cache()` is sufficient.

- **Q: What happens when Spark runs out of memory during a shuffle?**
  > Spark **spills to disk** — it writes the excess shuffle data to local disk on the executor. This is not a crash; Spark will still complete the job but will be slower due to disk I/O. You can see spill in the Spark UI's Stage task list under "Spill (Memory)" and "Spill (Disk)" columns.

### 🏢 Consultancy Scenario: "Executor Keeps Getting Killed"

**Scenario:** A client's job fails with "Container killed by YARN due to exceeding memory limits" even though `spark.executor.memory` seems adequate.

**Architect Answer:**
- `spark.executor.memory` is only the JVM heap. YARN sees total process memory = heap + overhead.
- The fix: increase `spark.executor.memoryOverhead` (default is only 10%).
- If using PySpark with heavy UDFs, the Python worker process also consumes memory in overhead.

```python
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.memoryOverhead", "2g")
# YARN container limit = 8 + 2 = 10g
```

### 🏛️ FAANG Scenario: "OOM in a Complex Aggregation"

**Scenario:** A job computes `groupBy("country").agg(collect_list("user_id"))` and crashes with OOM.

**Diagnosis:**
- `collect_list` collects ALL values into an array in one executor → if one country has 100M users, that array is huge.
- This is a combination of **data skew** + **memory-intensive aggregation**.

**Fix options:**
1. Increase `spark.sql.shuffle.partitions` to create smaller partitions
2. If one country dominates (e.g., US), pre-filter it and process separately
3. Replace `collect_list` with a size-bounded alternative if approximate results are acceptable

---

## ⚠️ Common Pitfalls

1. **Forgetting `unpersist()`** — cached DataFrames stay in memory until the SparkSession ends. In long-running jobs, this eats storage memory and causes spill elsewhere.

2. **Caching data used only once** — caching triggers a full job to populate the cache. If you only use the data once, you've paid double the cost (compute + cache write) with no benefit.

3. **Using `MEMORY_ONLY` for large datasets** — if the data doesn't fit, Spark drops the cached partitions and must recompute them on access. Use `MEMORY_AND_DISK` instead.

4. **Ignoring `memoryOverhead` for PySpark** — the Python process for each executor core runs outside the JVM. Heavy Python code can consume gigabytes of overhead memory, causing container kills.

5. **Relying on spill as a normal operating mode** — occasional spill is acceptable, but if your jobs routinely spill gigabytes, you have a partitioning or memory sizing problem that must be fixed.

---

## 🧪 Practice Exercises

### Exercise 1 — Calculate Memory Allocation (Beginner)
Given: `spark.executor.memory = 16g`, `spark.memory.fraction = 0.6`, `spark.memory.storageFraction = 0.5`

Calculate:
1. Reserved memory
2. Usable memory
3. Unified pool size
4. Storage soft limit
5. User memory

### Exercise 2 — Diagnose This OOM (Intermediate)
```
Error: "java.lang.OutOfMemoryError: GC overhead limit exceeded"
Spark UI shows: GC Time = 45% of task duration, Spill (Disk) = 50GB
```
What is the root cause and what are three possible fixes?

### Exercise 3 — Cache Strategy Decision (Advanced)
You have a DataFrame:
- 200GB in size
- Used in 5 different downstream transformations
- Recomputation cost: 45 minutes (complex aggregation)
- Executors have 32GB RAM each (20 executors = 640GB total)

What storage level would you use and why?

---

## 💼 Common Interview Questions

**Q1: Explain Spark's memory model. What are the different memory regions?**
> Executor memory is divided into: (1) **Reserved Memory** (~300MB, fixed), (2) **Unified Memory** (60% of remaining, shared between Execution and Storage), and (3) **User Memory** (40%, for Python objects and UDFs). The unified pool uses a "borrow" mechanism — execution can evict cached storage, and storage can use unused execution memory. On top of this, `memoryOverhead` allocates off-JVM memory for native code, Python workers, and OS.

**Q2: What is the difference between caching and checkpointing?**
> **Caching** stores data in executor memory (or disk) but keeps the full RDD lineage. If a cached partition is lost, Spark recomputes it from the lineage. **Checkpointing** saves data to HDFS and *breaks* the lineage. This is used to truncate very long lineage chains (preventing stack overflow) or to ensure recovery in Structured Streaming where recomputing from scratch would be too expensive.

**Q3: What causes an OOM error in the executor and how do you fix it?**
> Common causes: (1) data skew — one partition has far more data than others, (2) too few shuffle partitions — each task processes too much data, (3) caching too much data, (4) window functions holding a full partition in memory. Fixes: increase `spark.sql.shuffle.partitions`, fix skew with salting or AQE, increase `spark.executor.memory`, rewrite window aggregations as `groupBy + join`.

[← Lesson 5: Shuffle & Joins](../Lesson_5_Shuffle_and_Joins/README.md) | [Next: Lesson 7: Data Skew →](../Lesson_7_Data_Skew/README.md)
