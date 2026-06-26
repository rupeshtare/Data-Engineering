# why_spark_lab.py
# Phase 3, Lesson 1: Why Apache Spark?
# Goal: Demonstrate the power of Distributed Computing.

from pyspark.sql import SparkSession
import time

# 🏗️ Phase 1: Absolute Foundations (Beginner)
# Setup the Spark Session
spark = SparkSession.builder.appName("WhySparkLab").getOrCreate()

# Create a sample dataset that demonstrates distribution
data = [("User_" + str(i), i % 100) for i in range(1000)]
rdd = spark.sparkContext.parallelize(data)

# 🚀 Phase 2: Intermediate (Developer)
# Check how many 'Partitions' the data is split into
# Each partition is work that could happen on a different computer!
print(f"--- [DISTRIBUTION] Number of Partitions: {rdd.getNumPartitions()} ---")

# Simple map-reduce operation
result = rdd.count()
print(f"--- [RESULT] Total Rows Processed: {result} ---")

# 🏛️ Phase 3: Architect (Professional)
# Imagine this with 1000 partitions on 1000 machines.
# That is how Spark handles Petabytes of data.

# 🏛️ Architect's Tip:
# "Spark's power isn't in processing small data faster; it's in 
# processing MASSIVE data by breaking it into small pieces and 
# throwing MORE machines at it. This is called Horizontal Scaling."
