"""
Shared SparkSession factory for the Spark Mastery learning path.

Every exercise imports from here so the SAME code runs unchanged whether you are:
  - on your laptop  (local[*], or the local minikube Kubernetes cluster)
  - on the on-prem Kubernetes cluster (export SPARK_MASTER=k8s://https://<api-server>:6443)

Environment variables (all optional):
  SPARK_MASTER   default "local[*]"      e.g. "k8s://https://127.0.0.1:8443" (minikube) or a prod API server
  DATA_DIR       default "<repo>/data"    where generate_data.py wrote the parquet tables
                                          (on a cluster, an s3a:// path into MinIO/S3)
  ENABLE_ICEBERG "0"/"1", default "0"     adds Iceberg SQL extensions + a local filesystem catalog
  SPARK_EVENTLOG "0"/"1", default "1"     write event logs so the History Server (:18080) can replay runs

Usage inside an exercise:
    from common.spark_session import get_spark, read_table
    spark = get_spark("Day 10 - Data Skew")
    txns  = read_table(spark, "transactions")
"""

import os


def repo_root() -> str:
    """Absolute path to the repository root (the folder containing this `common/` package)."""
    return os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


def data_dir() -> str:
    """Directory holding the generated parquet tables. Override with DATA_DIR."""
    return os.environ.get("DATA_DIR", os.path.join(repo_root(), "data"))


def table_path(name: str) -> str:
    """Filesystem path of a generated table, e.g. table_path('transactions')."""
    return os.path.join(data_dir(), name)


def get_spark(app_name: str = "spark-learning", extra_conf: dict | None = None):
    """
    Build (or reuse) a SparkSession with learning-friendly defaults.

    Defaults are intentionally MODEST so behaviour on a laptop mirrors a small
    cluster: AQE on, 8 shuffle partitions, event logging on for the History Server.
    Override any of them via `extra_conf={"spark.sql.shuffle.partitions": "200"}`.
    """
    from pyspark.sql import SparkSession

    master = os.environ.get("SPARK_MASTER", "local[*]")

    builder = (
        SparkSession.builder.appName(app_name)
        .master(master)
        # Small, observable defaults. Many lessons deliberately change these.
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.autoBroadcastJoinThreshold", str(10 * 1024 * 1024))
        # Keep the UI history so you can inspect runs after they finish.
        .config("spark.ui.showConsoleProgress", "true")
    )

    if os.environ.get("SPARK_EVENTLOG", "1") == "1":
        events = os.environ.get("SPARK_EVENTLOG_DIR", os.path.join(repo_root(), "spark-events"))
        os.makedirs(events, exist_ok=True)
        builder = (
            builder.config("spark.eventLog.enabled", "true")
            .config("spark.eventLog.dir", _as_uri(events))
        )

    if os.environ.get("ENABLE_ICEBERG", "0") == "1":
        warehouse = os.environ.get("ICEBERG_WAREHOUSE", os.path.join(repo_root(), "iceberg-warehouse"))
        os.makedirs(warehouse, exist_ok=True)
        builder = (
            builder.config(
                "spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
            )
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
            # "hadoop" here is Iceberg's *filesystem* catalog implementation (it runs over
            # any Hadoop FileSystem, incl. local file:// and s3a://) — NOT the YARN/HDFS
            # stack. On the cluster point the warehouse at s3a://warehouse/iceberg instead.
            .config("spark.sql.catalog.local.type", "hadoop")
            .config("spark.sql.catalog.local.warehouse", _as_uri(warehouse))
        )

    for key, value in (extra_conf or {}).items():
        builder = builder.config(key, str(value))

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark


def read_table(spark, name: str):
    """Read a generated parquet table by name (e.g. 'transactions', 'customers')."""
    path = table_path(name)
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Table '{name}' not found at {path}.\n"
            f"Generate the sample data first:\n"
            f"    python environment/generate_data.py --scale small"
        )
    return spark.read.parquet(path)


def _as_uri(path: str) -> str:
    """Turn a local path into a file:// URI (Spark configs want URIs for dirs)."""
    p = os.path.abspath(path).replace("\\", "/")
    if not p.startswith("/"):
        p = "/" + p  # Windows drive paths -> file:///C:/...
    return "file://" + p
