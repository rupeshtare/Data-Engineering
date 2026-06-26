"""
Lesson 5 Lab: Shuffle & Joins
==============================
Hands-on experiments to understand shuffle cost and join strategies.
Run each section and observe the Spark UI at http://localhost:4040
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, broadcast, upper
import time

spark = SparkSession.builder \
    .appName("Lesson5_Shuffle_Joins") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("LAB 1: The Shuffle Cost — reduceByKey vs groupByKey")
print("=" * 60)

# Create a large-ish RDD to demonstrate the difference
import random
data = [(f"key_{random.randint(1, 100)}", random.randint(1, 100)) for _ in range(1_000_000)]
rdd = spark.sparkContext.parallelize(data, numSlices=8)

# ❌ Method 1: groupByKey (bad)
start = time.time()
result_gbk = rdd.groupByKey().mapValues(sum).count()
t_gbk = time.time() - start
print(f"groupByKey result count: {result_gbk}, time: {t_gbk:.2f}s")

# ✅ Method 2: reduceByKey (good)
start = time.time()
result_rbk = rdd.reduceByKey(lambda a, b: a + b).count()
t_rbk = time.time() - start
print(f"reduceByKey result count: {result_rbk}, time: {t_rbk:.2f}s")
print(f"reduceByKey speedup: {t_gbk / t_rbk:.1f}x faster")

print("\n" + "=" * 60)
print("LAB 2: Join Strategies")
print("=" * 60)

# Create test tables
large_data = [(i, f"user_{i}", i * 10) for i in range(100_000)]
small_data = [(i, f"region_{i % 10}") for i in range(100)]  # tiny lookup table

large_df = spark.createDataFrame(large_data, ["user_id", "name", "amount"])
small_df = spark.createDataFrame(small_data, ["user_id", "region"])

print("\n--- Join WITHOUT broadcast hint ---")
result = large_df.join(small_df, "user_id")
result.explain()  # Check for SortMergeJoin

print("\n--- Join WITH broadcast hint ---")
result_bc = large_df.join(broadcast(small_df), "user_id")
result_bc.explain()  # Check for BroadcastHashJoin — no shuffle!

print("\n" + "=" * 60)
print("LAB 3: shuffle.partitions Impact")
print("=" * 60)

# Too many partitions for small data
spark.conf.set("spark.sql.shuffle.partitions", "200")
start = time.time()
large_df.groupBy("region_bucket").agg(spark_sum("amount")).count()
t_200 = time.time() - start

# Right-sized partitions
spark.conf.set("spark.sql.shuffle.partitions", "8")
large_df2 = large_df.withColumn("region_bucket", col("user_id") % 10)
start = time.time()
large_df2.groupBy("region_bucket").agg(spark_sum("amount")).count()
t_8 = time.time() - start

print(f"200 shuffle partitions (too many for small data): {t_200:.2f}s")
print(f"  8 shuffle partitions (right-sized):            {t_8:.2f}s")

print("\n" + "=" * 60)
print("LAB 4: Avoid Expression in Join Keys")
print("=" * 60)

names_df = spark.createDataFrame([("Alice", 1), ("bob", 2), ("CAROL", 3)], ["name", "id"])
lookup_df = spark.createDataFrame([("alice", "NY"), ("bob", "LA"), ("carol", "SF")], ["name", "city"])

# ❌ Bad: expression in join condition — prevents hash join optimization
print("\n--- Join on expression (BAD) ---")
bad_join = names_df.join(lookup_df, upper(names_df.name) == upper(lookup_df.name))
bad_join.explain()

# ✅ Good: pre-compute column, then join
print("\n--- Join on pre-computed column (GOOD) ---")
names_normalized = names_df.withColumn("name_upper", upper(col("name")))
lookup_normalized = lookup_df.withColumn("name_upper", upper(col("name")))
good_join = names_normalized.join(lookup_normalized, "name_upper")
good_join.explain()

print("\n✅ Lab complete! Review Spark UI at http://localhost:4040")
print("Look at: Jobs → Stages → check shuffle read/write sizes")
spark.stop()
