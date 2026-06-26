# spark_architecture_lab.py
# Phase 3, Lesson 2: Spark Architecture
# Goal: Visualize the Driver vs. Executor relationship.

from pyspark.sql import SparkSession

# 🏗️ Phase 1: Absolute Foundations (Beginner)
# The Driver is the "Manager". It lives here in this Python script.
spark = SparkSession.builder \
    .appName("ArchitectureAudit") \
    .config("spark.executor.memory", "1g") \
    .getOrCreate()

print(f"--- [DRIVER] Application ID: {spark.sparkContext.applicationId} ---")

# 🚀 Phase 2: Intermediate (Developer)
# Let's perform a task that requires worker nodes
data = [i for i in range(1000000)]
df = spark.createDataset(data).toDF("num")

# Sum all numbers (This forces Executors to work)
total = df.agg({"num": "sum"}).collect()[0][0]
print(f"--- [EXECUTOR] Sum calculated by workers: {total} ---")

# 🏛️ Phase 3: Architect (Professional)
# Check the Spark UI (usually localhost:4040) to see the:
# 1. DAG (Directed Acyclic Graph)
# 2. Number of Tasks
# 3. Stage boundaries

# 🏛️ Architect's Tip:
# "The Driver is single-threaded and can become a bottleneck if 
# you 'collect()' too much data to it. Always keep the heavy 
# lifting (filter/sum/join) on the Executors and only bring 
# the small summaries back to the Driver."
