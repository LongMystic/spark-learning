"""
Synthetic dataset generator for the Spark Mastery learning path.

Writes Parquet tables into DATA_DIR (default: <repo>/data) so EVERY exercise
runs with zero access to the production cluster. It uses pure Spark (spark.range
+ SQL functions) to build the data, so the only dependency is pyspark itself.

Tables produced:
  customers            dimension  (medium cardinality)
  products             dimension  (small  -> broadcast-join candidate)
  stores               dimension  (tiny   -> broadcast-join candidate)
  transactions         fact       (large, evenly distributed, date-partitioned)
  transactions_skewed  fact       (large, ~80% of rows on 5 "hot" customers)

Usage:
    Run via Kubernetes (see environment/README.md)




Point exercises at the result via DATA_DIR (defaults to <repo>/data).
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from common.spark_session import get_spark, data_dir  # noqa: E402

from pyspark.sql import functions as F  # noqa: E402

SCALES = {
    "small": {"txns": 1_000_000, "customers": 50_000},
    "medium": {"txns": 10_000_000, "customers": 500_000},
    "large": {"txns": 50_000_000, "customers": 2_000_000},
}

N_PRODUCTS = 500      # small dimension -> fits in a broadcast
N_STORES = 50         # tiny dimension  -> fits in a broadcast
N_CATEGORIES = 12
N_DAYS = 60           # transactions spread across the last N days -> partitioning practice
HOT_CUSTOMERS = 5     # skewed table concentrates most rows on these ids


def build_customers(spark, n):
    return (
        spark.range(n)
        .withColumnRenamed("id", "customer_id")
        .withColumn("segment", F.element_at(
            F.array(F.lit("consumer"), F.lit("smb"), F.lit("enterprise")),
            (F.col("customer_id") % 3 + 1).cast("int")))
        .withColumn("city_id", (F.col("customer_id") % 200).cast("int"))
        .withColumn("signup_days_ago", (F.col("customer_id") % 1000).cast("int"))
        .withColumn("signup_date", F.date_sub(F.current_date(), F.col("signup_days_ago")))
        .withColumn("name", F.concat(F.lit("customer_"), F.col("customer_id")))
        .drop("signup_days_ago")
    )


def build_products(spark):
    return (
        spark.range(N_PRODUCTS)
        .withColumnRenamed("id", "product_id")
        .withColumn("category_id", (F.col("product_id") % N_CATEGORIES).cast("int"))
        .withColumn("category", F.concat(F.lit("category_"), F.col("category_id")))
        .withColumn("price", F.round(F.rand(seed=7) * 490 + 10, 2))  # 10..500
        .withColumn("product_name", F.concat(F.lit("product_"), F.col("product_id")))
    )


def build_stores(spark):
    regions = F.array(*[F.lit(r) for r in ["north", "south", "east", "west", "central"]])
    return (
        spark.range(N_STORES)
        .withColumnRenamed("id", "store_id")
        .withColumn("region", F.element_at(regions, (F.col("store_id") % 5 + 1).cast("int")))
        .withColumn("store_name", F.concat(F.lit("store_"), F.col("store_id")))
    )


def _base_transactions(spark, n_txns, n_customers, customer_expr):
    """Shared transaction builder; `customer_expr` decides the customer_id distribution.

    The fact table is deliberately DENORMALIZED with `category` and `status` columns
    (in addition to the star-schema foreign keys) so simple groupBy/filter exercises
    run without a join, while join exercises can still use customer_id / product_id.
    """
    status_arr = F.array(*[F.lit(s) for s in ["active", "completed", "cancelled", "refunded"]])
    return (
        spark.range(n_txns)
        .withColumnRenamed("id", "txn_id")
        .withColumn("customer_id", customer_expr)
        .withColumn("product_id", (F.col("txn_id") % N_PRODUCTS).cast("long"))
        .withColumn("store_id", (F.col("txn_id") % N_STORES).cast("long"))
        .withColumn("category", F.concat(F.lit("category_"), (F.col("txn_id") % N_CATEGORIES).cast("int")))
        .withColumn("status", F.element_at(status_arr, (F.col("txn_id") % 4 + 1).cast("int")))
        .withColumn("quantity", (F.col("txn_id") % 5 + 1).cast("int"))
        .withColumn("amount", F.round(F.rand(seed=13) * 200 + 1, 2))
        .withColumn("days_ago", (F.col("txn_id") % N_DAYS).cast("int"))
        .withColumn("txn_ts", F.expr("current_timestamp() - make_interval(0,0,0,days_ago)"))
        .withColumn("txn_date", F.to_date(F.col("txn_ts")))
        .drop("days_ago")
    )


def build_transactions(spark, n_txns, n_customers):
    # Even distribution: customer_id spread uniformly across the whole customer base.
    even = (F.rand(seed=101) * n_customers).cast("long")
    return _base_transactions(spark, n_txns, n_customers, even)


def build_transactions_skewed(spark, n_txns, n_customers):
    # ~80% of rows land on the first HOT_CUSTOMERS ids; the rest spread out.
    hot = F.rand(seed=202) < F.lit(0.80)
    hot_id = (F.rand(seed=303) * HOT_CUSTOMERS).cast("long")
    cold_id = (F.rand(seed=404) * n_customers).cast("long")
    skewed = F.when(hot, hot_id).otherwise(cold_id)
    return _base_transactions(spark, n_txns, n_customers, skewed)


def _is_uri(path: str) -> bool:
    """True for object-store / distributed paths like s3a://... or file://... (not a local dir)."""
    return "://" in path


def _join(out_dir: str, name: str) -> str:
    """Join a table name onto out_dir, keeping URI paths ('/') intact on every OS."""
    if _is_uri(out_dir):
        return out_dir.rstrip("/") + "/" + name
    return os.path.join(out_dir, name)


def write(df, name, out_dir, overwrite, partition_by=None):
    path = _join(out_dir, name)
    writer = df.write.mode("overwrite" if overwrite else "errorifexists")
    if partition_by:
        writer = writer.partitionBy(partition_by)
    writer.parquet(path)
    print(f"  wrote {name:22s} -> {path}")


def main():
    parser = argparse.ArgumentParser(description="Generate sample Spark datasets.")
    parser.add_argument("--scale", choices=list(SCALES), default="small")
    parser.add_argument("--overwrite", action="store_true", help="overwrite existing tables")
    parser.add_argument("--out", default=None, help="output dir (default: DATA_DIR or <repo>/data)")
    args = parser.parse_args()

    out_dir = args.out or data_dir()
    # Only pre-create local directories; object stores (s3a://) create prefixes lazily.
    if not _is_uri(out_dir):
        os.makedirs(out_dir, exist_ok=True)
    cfg = SCALES[args.scale]

    spark = get_spark(f"generate-data-{args.scale}", extra_conf={"spark.sql.shuffle.partitions": "16"})
    print(f"Generating '{args.scale}' dataset into {out_dir}")
    print(f"  transactions rows: {cfg['txns']:,}   customers: {cfg['customers']:,}")

    write(build_customers(spark, cfg["customers"]), "customers", out_dir, args.overwrite)
    write(build_products(spark), "products", out_dir, args.overwrite)
    write(build_stores(spark), "stores", out_dir, args.overwrite)
    write(build_transactions(spark, cfg["txns"], cfg["customers"]),
          "transactions", out_dir, args.overwrite, partition_by="txn_date")
    write(build_transactions_skewed(spark, cfg["txns"], cfg["customers"]),
          "transactions_skewed", out_dir, args.overwrite, partition_by="txn_date")

    print("Done. Point exercises at this data with DATA_DIR (already the default).")
    spark.stop()


if __name__ == "__main__":
    main()
