"""
Exercise 1: DAG Analysis
Purpose: Understand Spark execution model by analyzing DAGs

Instructions:
1. Run this script on your Spark cluster
2. Open Spark UI (http://driver:4040)
3. Analyze the DAG for each query
4. Count the number of stages
5. Identify shuffle operations
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql.functions import col, sum as spark_sum, count

# Initialize Spark Session
spark = get_spark("DAG Analysis Exercise")

# Exercise 1: Simple transformation (should create 1 stage)
print("=" * 50)
print("Exercise 1: Simple Filter and Select")
print("=" * 50)

df = read_table(spark, "transactions")

result1 = df.filter(col("amount") > 100).select("customer_id", "amount")

# View the execution plan
print("\nExecution Plan:")
result1.explain(extended=True)

# Trigger execution and check Spark UI
print("count:", result1.count())

# Exercise 2: Join operation (should create multiple stages)
print("\n" + "=" * 50)
print("Exercise 2: Join Operation")
print("=" * 50)

df1 = read_table(spark, "transactions")
df2 = read_table(spark, "customers")

result2 = df1.join(df2, "customer_id").filter(col("status") == "active")

print("\nExecution Plan:")
result2.explain(extended=True)

# Trigger execution and check Spark UI
# result2.count()  # Uncomment to execute

# Exercise 3: GroupBy operation (should create multiple stages)
print("\n" + "=" * 50)
print("Exercise 3: GroupBy with Aggregation")
print("=" * 50)

result3 = df.groupBy("category").agg(
    spark_sum("amount").alias("total"),
    count("*").alias("count")
).orderBy("total", ascending=False)

print("\nExecution Plan:")
result3.explain(extended=True)

# Trigger execution and check Spark UI
result3.show(10)

# Exercise 4: Complex query (multiple stages)
print("\n" + "=" * 50)
print("Exercise 4: Complex Multi-Stage Query")
print("=" * 50)

# TODO: Create a query with at least 3 stages
# Hint: Use filter, join, and groupBy operations

# Your code here:
# result4 = ...

# print("\nExecution Plan:")
# result4.explain(extended=True)

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. How many stages does each query create?")
print("2. Which operations cause stage boundaries?")
print("3. What is the difference between narrow and wide dependencies?")
print("4. Check Spark UI - what is the task distribution?")
print("5. What is the data locality for tasks?")

spark.stop()

