# pyspark_zero_to_hero.py
# Zero-to-Hero: From reading a file to complex aggregations.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg

# 1. THE ARCHITECT'S SETUP
spark = SparkSession.builder.appName("ZeroToHero").getOrCreate()

# 2. BEGINNER: Reading Data
# Let's create some dummy data since we don't have a CSV file yet
raw_data = [("Laptop", "Electronics", 1200), ("Phone", "Electronics", 800), 
            ("Desk", "Furniture", 450), ("Chair", "Furniture", 200)]
df = spark.createDataFrame(raw_data, ["product", "category", "price"])

# 3. INTERMEDIATE: Transformations
print("--- [FILTERING] Expensive items ---")
expensive_df = df.filter(col("price") > 500)
expensive_df.show()

# 4. ARCHITECT: Grouping & Aggregation
print("--- [AGGREGATING] Average price by category ---")
agg_df = df.groupBy("category").agg(avg("price").alias("avg_price"))
agg_df.show()

# 5. THE ARCHITECT'S FINISH: Writing as Delta
# (This would work on Databricks/Delta Lake)
# agg_df.write.format("delta").saveAsTable("gold_category_metrics")

# 🏛️ Architect's Tip:
# "Data quality is your responsibility. Always run a count() or 
# a schema check before you write your data. If the aggregation 
# results look impossible, stop the job before it hits the dashboard!"
