"""
Exercise 21: Production Incident Response
Purpose: Make a write IDEMPOTENT so "re-run the job" is a safe mitigation.

# Run via Kubernetes (update and apply environment/k8s/05-example-sparkapplication.yaml)
Pair with interview-prep/incident-drills.md for the decision-making practice.
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F

spark = get_spark("Incident Response")
txns = read_table(spark, "transactions")

base = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data"))
out = os.path.join(base, "_ex21_daily")

print("=" * 60)
print("Idempotent write: dynamic partition overwrite")
print("=" * 60)
# Only the partitions PRESENT in the written data are replaced, so re-running the
# job for one bad date does NOT wipe the rest of the table.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

daily = txns.groupBy("txn_date", "category").agg(F.sum("amount").alias("total"))

print("  First run (writes all dates)...")
daily.write.mode("overwrite").partitionBy("txn_date").parquet(out)
count1 = spark.read.parquet(out).count()
print(f"  rows after run 1: {count1}")

print("\n  Re-run (simulating a mitigation re-run) -- should NOT duplicate rows...")
daily.write.mode("overwrite").partitionBy("txn_date").parquet(out)
count2 = spark.read.parquet(out).count()
print(f"  rows after run 2: {count2}  (equal to run 1 => idempotent ✅)")

assert count1 == count2, "Write was NOT idempotent!"

print("\n" + "=" * 60)
print("Incident playbook (walk through mentally for a scripted outage)")
print("=" * 60)
print("""
  1. ACKNOWLEDGE  who/what/impact/since-when
  2. ASSESS       failing | hung | slow?  which stage? (Spark UI / kubectl logs + get pods)
  3. STABILIZE    fastest safe mitigation (idempotent re-run, namespace w/ free quota,
                  AQE skew join, quarantine bad partition, serve last snapshot)
  4. DIAGNOSE     root cause via Days 15-20
  5. FIX          permanent change + test
  6. POSTMORTEM   blameless timeline + concrete prevention
""")
print("Analysis Questions")
print("1. Why is dynamic partition overwrite safer to re-run than full overwrite?")
print("2. Name a mitigation for: queue starvation / sudden skew / bad upstream data.")
print("3. What must be true about a job before 're-run it' is a safe first move?")

spark.stop()
