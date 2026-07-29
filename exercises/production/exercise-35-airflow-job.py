"""
Exercise 35: Airflow-style parameterized, idempotent Spark job
Purpose: A --run-date-driven job that writes only that date's partition, so
Airflow retries and backfills are safe. This is exactly what a SparkSubmitOperator
would launch with application_args=["--run-date", "{{ ds }}"].

# Run via Kubernetes (update and apply environment/k8s/05-example-sparkapplication.yaml)
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

from pyspark.sql import functions as F


def main(run_date):
    spark = get_spark(f"daily-etl-{run_date}")
    # Idempotent write: only the given date's partition is (re)written.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

    txns = read_table(spark, "transactions")
    day = txns.where(F.col("txn_date") == F.lit(run_date))
    daily = day.groupBy("txn_date", "category").agg(F.sum("amount").alias("total"))

    base = os.environ.get("DATA_DIR", os.path.join(os.path.dirname(__file__), "..", "..", "data"))
    out = os.path.join(base, "_ex35_daily_sales")

    n = daily.count()
    print(f"run-date={run_date}: {n} category rows")
    if n == 0:
        print("  (no rows for that date in the sample data; try a date within the last 60 days)")
    daily.write.mode("overwrite").partitionBy("txn_date").parquet(out)
    print(f"  wrote partition txn_date={run_date} to {out}")
    print("  Re-running with the SAME --run-date is safe (dynamic overwrite replaces only that date).")

    spark.stop()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--run-date", required=True, help="logical date, e.g. Airflow {{ ds }}")
    args = ap.parse_args()
    main(args.run_date)

# Analysis Questions
# 1. Why does partitionBy + dynamic overwrite make retries/backfills safe?
# 2. What would the SparkSubmitOperator definition look like for this job?
# 3. Why should the transform NOT run inside the Airflow worker process?
