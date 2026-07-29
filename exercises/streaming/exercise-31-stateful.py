"""
Exercise 31: Stateful Streaming — event-time windows + watermark (rate source)
Purpose: Windowed aggregation with a watermark; observe bounded state.

# Run via Kubernetes (update and apply environment/k8s/05-example-sparkapplication.yaml)
Uses the built-in 'rate' source (rows with a timestamp), so no Kafka is required.
The query runs for ~20s then stops.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark

from pyspark.sql import functions as F

spark = get_spark("Stateful Streaming")
chk = tempfile.mkdtemp(prefix="stateful_chk_")

# rate source: columns (timestamp, value); 20 rows/sec
rate = (spark.readStream.format("rate").option("rowsPerSecond", 20).load())

events = rate.withColumn("customer_id", (F.col("value") % 10))

print("=" * 60)
print("Event-time windowed aggregation with a 10s watermark")
print("=" * 60)
windowed = (events
    .withWatermark("timestamp", "10 seconds")
    .groupBy(F.window("timestamp", "5 seconds"), "customer_id")
    .agg(F.count("*").alias("events"), F.sum("value").alias("sum_value")))

query = (windowed.writeStream
    .outputMode("update")
    .format("console")
    .option("truncate", "false")
    .option("checkpointLocation", chk)
    .trigger(processingTime="5 seconds")
    .start())

# Run for ~20 seconds, then stop cleanly.
query.awaitTermination(20)
query.stop()

print("\nAnalysis Questions")
print("1. What does the watermark let Spark do to old window STATE?")
print("2. What happens to an event later than the watermark?")
print("3. In the Structured Streaming UI tab, what were the state-store metrics?")

spark.stop()
