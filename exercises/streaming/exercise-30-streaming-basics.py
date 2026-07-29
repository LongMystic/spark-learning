"""
Exercise 30: Structured Streaming Fundamentals (no Kafka needed)
Purpose: Stream from a directory of Parquet files using trigger(availableNow),
aggregate, and inspect the checkpoint. This is the "streaming-as-batch" pattern.

# Run via Kubernetes (update and apply environment/k8s/05-example-sparkapplication.yaml)
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F

spark = get_spark("Streaming Basics")

# Prepare a source directory by splitting the batch table into a few files.
src = tempfile.mkdtemp(prefix="stream_src_")
chk = tempfile.mkdtemp(prefix="stream_chk_")
print(f"source dir: {src}")

txns = read_table(spark, "transactions").select("customer_id", "amount", "category", "txn_ts")
txns.repartition(4).write.mode("overwrite").parquet(src)

schema = txns.schema  # streaming file source requires an explicit schema

print("=" * 60)
print("Streaming read -> aggregate -> memory sink, trigger(availableNow)")
print("=" * 60)
stream = (spark.readStream
    .schema(schema)
    .option("maxFilesPerTrigger", 1)   # process one file per micro-batch
    .parquet(src))

agg = stream.groupBy("category").agg(F.sum("amount").alias("total"))

query = (agg.writeStream
    .outputMode("complete")
    .format("memory")
    .queryName("cat_totals")
    .option("checkpointLocation", chk)
    .trigger(availableNow=True)      # process all currently-available files, then stop
    .start())
query.awaitTermination()

print("\nResult after draining all files:")
spark.sql("SELECT * FROM cat_totals ORDER BY category").show()

print(f"\nCheckpoint contents (offsets/commits) under: {chk}")
for root, _, files in os.walk(chk):
    for f in files:
        print("  ", os.path.relpath(os.path.join(root, f), chk))

print("\nAnalysis Questions")
print("1. What does trigger(availableNow) do vs processingTime?")
print("2. What is stored in the checkpoint, and why does it enable recovery?")
print("3. Why does aggregation here use outputMode('complete')?")

spark.stop()
