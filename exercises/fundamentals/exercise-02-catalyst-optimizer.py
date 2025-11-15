"""
Exercise 2: Catalyst Optimizer Analysis
Purpose: Understand how Catalyst optimizer transforms queries

Instructions:
1. Run queries with different optimization settings
2. Compare execution plans
3. Measure performance differences
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum

spark = SparkSession.builder \
    .appName("Catalyst Optimizer Exercise") \
    .getOrCreate()

# Exercise 1: Compare Logical vs Optimized Plans
print("=" * 50)
print("Exercise 1: Plan Comparison")
print("=" * 50)

df = spark.read.parquet("your_table_path/")

# Query with nested structure
result = df.filter(col("age") > 25).select("name", "age", "salary")

print("\n=== Logical Plan ===")
print(result.queryExecution.logical)

print("\n=== Optimized Logical Plan ===")
print(result.queryExecution.optimizedPlan)

print("\n=== Physical Plan ===")
print(result.queryExecution.executedPlan)

# Exercise 2: Predicate Pushdown Analysis
print("\n" + "=" * 50)
print("Exercise 2: Predicate Pushdown")
print("=" * 50)

# Query that should benefit from predicate pushdown
query = """
SELECT * FROM (
    SELECT * FROM large_table
) WHERE status = 'active' AND age > 25
"""

result2 = spark.sql(query)

print("\nExecution Plan (check for predicate pushdown):")
result2.explain(extended=True)

# Exercise 3: Column Pruning
print("\n" + "=" * 50)
print("Exercise 3: Column Pruning")
print("=" * 50)

# Select only needed columns
result3 = df.select("id", "name").filter(col("status") == "active")

print("\nExecution Plan (should only read id, name, status):")
result3.explain(extended=True)

# Exercise 4: Enable/Disable Optimizations
print("\n" + "=" * 50)
print("Exercise 4: Optimization Impact")
print("=" * 50)

# Disable predicate pushdown
spark.conf.set("spark.sql.optimizer.predicatePushdown.enabled", "false")

result4 = df.filter(col("age") > 25)
print("\nPlan WITHOUT predicate pushdown:")
result4.explain(extended=True)

# Re-enable
spark.conf.set("spark.sql.optimizer.predicatePushdown.enabled", "true")

result5 = df.filter(col("age") > 25)
print("\nPlan WITH predicate pushdown:")
result5.explain(extended=True)

# Exercise 5: Cost-Based Optimization
print("\n" + "=" * 50)
print("Exercise 5: CBO Analysis")
print("=" * 50)

# Enable CBO
spark.conf.set("spark.sql.cbo.enabled", "true")
spark.conf.set("spark.sql.cbo.joinReorder.enabled", "true")

# TODO: Collect statistics first
# spark.sql("ANALYZE TABLE table_name COMPUTE STATISTICS FOR ALL COLUMNS")

# Join query
df1 = spark.read.parquet("table1_path/")
df2 = spark.read.parquet("table2_path/")

result6 = df1.join(df2, "id")

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

