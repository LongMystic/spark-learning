"""
Exercise 40: Cost / Observability review
Purpose: Run a job, then read its metrics to fill a simple resource/waste scorecard.
Event logging is on by default (common/spark_session.py) so the History Server
(:18080 in the docker env) can replay this run.

Run:  python exercises/production/exercise-40-cost-observability.py
"""

import os
import sys
import time

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table, repo_root

from pyspark.sql import functions as F

spark = get_spark("Cost & Observability")
txns = read_table(spark, "transactions")

print("=" * 60)
print("Run a representative job and time it")
print("=" * 60)
start = time.time()
res = (txns.where(F.col("status") == "active")
           .groupBy("category", "store_id")
           .agg(F.sum("amount").alias("total"), F.count("*").alias("n")))
rows = res.count()
elapsed = time.time() - start
sc = spark.sparkContext

print(f"  result rows        : {rows}")
print(f"  wall-clock         : {elapsed:.2f}s")
print(f"  default parallelism: {sc.defaultParallelism}")
print(f"  app id             : {sc.applicationId}")

print("\n" + "=" * 60)
print("Cost / waste scorecard — inspect in the Spark UI / History Server")
print("=" * 60)
print(f"""
  Event logs: {os.path.join(repo_root(), 'spark-events')}
  History Server (docker env): http://localhost:18080

  Fill this in from the SQL tab + Stages tab for the run above:
    [ ] Biggest stage & its share of total task time?
    [ ] Max/median task-duration ratio (skew)?
    [ ] Any Spill (Memory/Disk)?
    [ ] Bytes read vs result size (full scan / small files)?
    [ ] Were executors idle between stages (over-allocated)?

  Optimization loop:  measure -> biggest waste -> fix ONE thing -> re-measure.
""")
print("Analysis Questions")
print("1. What is 'cost' on a shared on-prem cluster, concretely?")
print("2. Which single change would most reduce this job's resources-held?")
print("3. Which leading indicators would you alert on before an SLA breach?")

spark.stop()
