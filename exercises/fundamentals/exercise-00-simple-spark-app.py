import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark

from pyspark.sql.functions import col, sum as spark_sum, count

spark = get_spark("Simple Spark Application")

columns = ["id", "amount"]

data = [(1, 1), (1, 2), (2, 2), (3, 1), (3, 0), (3, 5), (3, 10)]

df = spark.createDataFrame(data, columns)

print("=" * 30)
print("Simple Spark DataFrame")
df.show()

print("=" * 30)
aggregated_df = (
    df.groupBy("id")
        .sum("amount")
        .select(
            "id",
            col("sum(amount)").alias("total_amount"),
        )
)
print("Aggregated DataFrame")
aggregated_df.show()

aggregated_df.explain(True)
print("=" * 30)
print("FINISHED")

spark.stop()