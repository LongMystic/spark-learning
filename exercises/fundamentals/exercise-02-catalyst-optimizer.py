"""
Exercise 2: Catalyst Optimizer Analysis
Purpose: Understand how Catalyst optimizer transforms queries

Instructions:
1. Run queries with different optimization settings
2. Compare execution plans
3. Measure performance differences
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql.functions import col, sum as spark_sum

spark = get_spark("Catalyst Optimizer Exercise")

# Exercise 1: Compare Logical vs Optimized Plans
print("=" * 50)
print("Exercise 1: Plan Comparison")
print("=" * 50)

df = read_table(spark, "transactions")
df.createOrReplaceTempView("transactions")

# Query with nested structure
result = df.filter(col("amount") > 25).select("customer_id", "amount", "quantity")

# Note: in PySpark the plans live under result._jdf.queryExecution().*
print("\n=== Plans (logical -> optimized -> physical) ===")
result.explain(mode="extended")

# Exercise 2: Predicate Pushdown Analysis
print("\n" + "=" * 50)
print("Exercise 2: Predicate Pushdown")
print("=" * 50)

# Query that should benefit from predicate pushdown
query = """
SELECT * FROM (
    SELECT * FROM transactions
) WHERE status = 'active' AND amount > 25
"""

result2 = spark.sql(query)

print("\nExecution Plan (check for predicate pushdown):")
result2.explain(extended=True)

# Exercise 3: Column Pruning
print("\n" + "=" * 50)
print("Exercise 3: Column Pruning")
print("=" * 50)

# Select only needed columns
result3 = df.select("customer_id", "amount").filter(col("status") == "active")

print("\nExecution Plan (should only read customer_id, amount, status):")
result3.explain(extended=True)

# Exercise 4: Enable/Disable Optimizations
print("\n" + "=" * 50)
print("Exercise 4: Optimization Impact")
print("=" * 50)

# Disable predicate pushdown
spark.conf.set("spark.sql.optimizer.predicatePushdown.enabled", "false")

result4 = df.filter(col("amount") > 25)
print("\nPlan WITHOUT predicate pushdown:")
result4.explain(extended=True)

# Re-enable
spark.conf.set("spark.sql.optimizer.predicatePushdown.enabled", "true")

result5 = df.filter(col("amount") > 25)
print("\nPlan WITH predicate pushdown:")
result5.explain(extended=True)

# Exercise 5: Cost-Based Optimization
print("\n" + "=" * 50)
print("Exercise 5: CBO Analysis")
print("=" * 50)

# Enable CBO
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

# TODO: Collect statistics first (works on managed tables; see Day 28)
# spark.sql("ANALYZE TABLE transactions COMPUTE STATISTICS FOR ALL COLUMNS")

# Join query
df1 = read_table(spark, "transactions")
df2 = read_table(spark, "customers")

result6 = df1.join(df2, "customer_id")

print("\nExecution Plan with CBO:")
result6.explain(extended=True)

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. What optimizations are applied in the optimized plan?")
print("2. How does predicate pushdown affect the plan?")
print("3. What columns are actually read (column pruning)?")
print("4. How does disabling optimizations affect performance?")
print("5. What is the impact of CBO on join ordering?")

spark.stop()

