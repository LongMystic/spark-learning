"""
Exercise 5: Partitioning Strategies
Purpose: Understand partitioning and partition pruning

Instructions:
1. Create partitioned tables
2. Test partition pruning
3. Compare partition strategies
4. Handle small file problems
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month, dayofmonth
import time

spark = SparkSession.builder \
    .appName("Partitioning Strategies Exercise") \
    .getOrCreate()

# Exercise 1: Create Partitioned Table
print("=" * 50)
print("Exercise 1: Create Partitioned Table")
print("=" * 50)

# Create data with date columns
df = spark.range(0, 1000000).withColumn(
    "date", col("id").cast("timestamp")
).withColumn("year", year(col("date"))) \
 .withColumn("month", month(col("date"))) \
 .withColumn("day", dayofmonth(col("date"))) \
 .withColumn("amount", (col("id") % 1000) + 1)

# Write partitioned by year and month
print("Writing partitioned data...")
df.write.mode("overwrite").partitionBy("year", "month").parquet("partitioned_data/")

print("Partitioned data written to: partitioned_data/")
print("  Structure: year=YYYY/month=MM/data.parquet")

# Exercise 2: Test Partition Pruning
print("\n" + "=" * 50)
print("Exercise 2: Partition Pruning")
print("=" * 50)

# Query with partition filter (should prune)
print("--- Query with Partition Filter ---")
start = time.time()
df_filtered = spark.read.parquet("partitioned_data/") \
    .filter((col("year") == 2023) & (col("month") == 1))
# df_filtered.count()  # Uncomment to execute
time_pruned = time.time() - start

print(f"  Execution time: {time_pruned:.2f}s")
print("  Check explain plan for PartitionFilters")

df_filtered.explain(extended=True)

# Query without partition filter (reads all partitions)
print("\n--- Query without Partition Filter ---")
start = time.time()
df_all = spark.read.parquet("partitioned_data/")
# df_all.count()  # Uncomment to execute
time_all = time.time() - start

print(f"  Execution time: {time_all:.2f}s")
print("  Reads all partitions")

print(f"\nImprovement: {((time_all - time_pruned) / time_all * 100):.1f}%")

# Exercise 3: Compare Partition Granularity
print("\n" + "=" * 50)
print("Exercise 3: Partition Granularity")
print("=" * 50)

# Strategy 1: Only year
print("--- Strategy 1: Only Year ---")
df.write.mode("overwrite").partitionBy("year").parquet("strategy1/")
print("  Partitions: year only")

# Strategy 2: year, month
print("\n--- Strategy 2: Year, Month ---")
df.write.mode("overwrite").partitionBy("year", "month").parquet("strategy2/")
print("  Partitions: year, month")

# Strategy 3: year, month, day
print("\n--- Strategy 3: Year, Month, Day ---")
df.write.mode("overwrite").partitionBy("year", "month", "day").parquet("strategy3/")
print("  Partitions: year, month, day")

print("""
Compare:
- Number of partitions
- File sizes
- Query performance
- Partition pruning effectiveness
""")

# Exercise 4: Dynamic vs Static Partition Overwrite
print("\n" + "=" * 50)
print("Exercise 4: Dynamic Partition Overwrite")
print("=" * 50)

# Static overwrite (overwrites all partitions)
print("--- Static Overwrite ---")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
df.write.mode("overwrite").partitionBy("year", "month").parquet("partitioned_data/")
print("  Overwrites all partitions")

# Dynamic overwrite (only overwrites partitions in data)
print("\n--- Dynamic Overwrite ---")
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
df.filter(col("year") == 2023).write.mode("overwrite").partitionBy("year", "month").parquet("partitioned_data/")
print("  Only overwrites partitions present in data")

# Exercise 5: Handle Small Files
print("\n" + "=" * 50)
print("Exercise 5: Handle Small Files")
print("=" * 50)

# Create many small files
print("Creating many small files...")
for i in range(10):
    df.filter(col("id") % 10 == i).write.mode("append") \
      .partitionBy("year", "month").parquet("small_files/")

print("  Created many small files")
print("  Check file count and sizes")

# Coalesce to reduce files
print("\nCoalescing files...")
df_coalesced = spark.read.parquet("small_files/") \
    .repartition(10, "year", "month")
df_coalesced.write.mode("overwrite") \
    .partitionBy("year", "month").parquet("coalesced_files/")

print("  Files coalesced")
print("  Compare file count: before vs after")

# Exercise 6: Partition Column Selection
print("\n" + "=" * 50)
print("Exercise 6: Partition Column Selection")
print("=" * 50)

print("Good partition columns:")
print("  - High cardinality (many distinct values)")
print("  - Frequently used in WHERE clauses")
print("  - Balanced data distribution")
print("  - 2-3 columns typically")

print("\nBad partition columns:")
print("  - Low cardinality (few distinct values)")
print("  - Very high cardinality (millions of values)")
print("  - Skewed distribution")
print("  - Too many columns (5+)")

# Exercise 7: Hive Table Partitioning
print("\n" + "=" * 50)
print("Exercise 7: Hive Table Partitioning")
print("=" * 50)

# Create partitioned Hive table
print("Creating partitioned Hive table...")
spark.sql("""
    CREATE TABLE IF NOT EXISTS partitioned_sales (
        id BIGINT,
        amount DECIMAL(10,2)
    ) PARTITIONED BY (year INT, month INT)
    STORED AS PARQUET
""")

# Insert data
df.select("id", "amount", "year", "month").write.mode("append").insertInto("partitioned_sales")

print("  Partitioned Hive table created")
print("  Query with partition filters for pruning")

# Query with partition pruning
result = spark.sql("""
    SELECT * FROM partitioned_sales
    WHERE year = 2023 AND month = 1
""")

result.explain(extended=True)
print("  Check explain plan for PartitionFilters")

print("\n" + "=" * 50)
print("Analysis Questions:")
print("=" * 50)
print("1. How does partition pruning improve performance?")
print("2. What is the optimal partition granularity?")
print("3. What are the trade-offs of dynamic vs static overwrite?")
print("4. How do you handle small file problems?")
print("5. What makes a good partition column?")
print("6. Check explain plans - when does partition pruning occur?")

spark.stop()

