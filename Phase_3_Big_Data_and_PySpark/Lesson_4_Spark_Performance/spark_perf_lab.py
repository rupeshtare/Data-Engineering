# spark_perf_lab.py
# Phase 3, Lesson 4: Spark Performance
# Goal: Master the 'Broadcast Join' to kill the Shuffle.

from pyspark.sql import SparkSession
from pyspark.sql.functions import broadcast

# 🏗️ Phase 1: Absolute Foundations (Beginner)
spark = SparkSession.builder.appName("PerformanceLab").getOrCreate()

# Create a Large Table (Sales) and a Small Table (Store Locations)
large_data = [(i, "Product_" + str(i % 100), i * 1.5, i % 10) for i in range(10000)]
small_data = [(i, "Store_" + str(i), "Region_" + str(i % 5)) for i in range(10)]

df_sales = spark.createDataFrame(large_data, ["sale_id", "product", "price", "store_id"])
df_stores = spark.createDataFrame(small_data, ["store_id", "store_name", "region"])

# 🚀 Phase 2: Intermediate (Developer)
# Normal Join (Causes a Shuffle)
print("--- [SHUFFLE] Performing Standard Join ---")
df_joined = df_sales.join(df_stores, "store_id")
# df_joined.explain() # See the 'SortMergeJoin'

# 🏛️ Phase 3: Architect (Professional)
# Broadcast Join (No Shuffle!)
print("--- [BROADCAST] Performing Optimized Join ---")
df_optimized = df_sales.join(broadcast(df_stores), "store_id")
# df_optimized.explain() # See the 'BroadcastHashJoin'

# 🏛️ Architect's Tip:
# "A Shuffle is like moving every student in the school to a new 
# classroom based on their last name. A Broadcast is like printing 
# a map and giving it to EVERY student. If the map (small table) 
# is small enough (under 10MB), Broadcast is ALWAYS faster!"
