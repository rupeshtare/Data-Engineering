# spark_lab_basics.py
# Beginner Level: The absolute basics to get started.

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# 1. SETUP
spark = SparkSession.builder.appName("BeginnerLab").getOrCreate()

# 2. CREATE (Example Data)
data = [("James","Sales","NY",90000),
        ("Michael","Sales","NY",86000),
        ("Robert","Sales","CA",81000),
        ("Maria","Finance","CA",90000)]

columns= ["employee_name","department","state","salary"]
df = spark.createDataFrame(data = data, schema = columns)

# 3. READ
print("--- [READING] Full Dataset ---")
df.show()

# 4. TRANSFORM (UPDATE logic)
print("--- [TRANSFORMING] High Earners ---")
df_high = df.filter(col("salary") > 85000)
df_high.show()

# 5. AGGREGATE (Architect Level thinking)
print("--- [AGGREGATING] Salary by Dept ---")
df_agg = df.groupBy("department").avg("salary")
df_agg.show()

# 🏛️ Architect's Tip:
# "Notice how Spark doesn't do the work until you call '.show()'. 
# This is called 'Lazy Evaluation'. It allows Spark to optimize 
# the entire plan before executing it."
