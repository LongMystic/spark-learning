"""
Exercise 29: Spark Thrift Server (concept + local simulation)
Purpose: Understand the shared-session model and incremental collection; learn the
beeline connection + configs you'd use on the cluster.

Run:  python exercises/production/exercise-29-thrift-server.py
"""

import os
import sys

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "..")))
from common.spark_session import get_spark, read_table

# The Thrift Server shares ONE SparkContext across all users. Locally we simulate
# "sessions" with newSession(), which share the same underlying SparkContext.
spark = get_spark("Thrift Server (simulated)")
read_table(spark, "transactions").createOrReplaceGlobalTempView("transactions")

print("=" * 60)
print("Shared context, isolated sessions")
print("=" * 60)
s1 = spark.newSession()
s2 = spark.newSession()
s1.conf.set("spark.sql.shuffle.partitions", "8")
s2.conf.set("spark.sql.shuffle.partitions", "64")
print("  session1 shuffle.partitions:", s1.conf.get("spark.sql.shuffle.partitions"))
print("  session2 shuffle.partitions:", s2.conf.get("spark.sql.shuffle.partitions"))
print("  -> per-session SQL confs, but ONE shared SparkContext/executors (like STS).")

print("\n--- Both sessions query the shared global temp view ---")
for name, s in [("s1", s1), ("s2", s2)]:
    r = s.sql("SELECT category, COUNT(*) c FROM global_temp.transactions GROUP BY category")
    print(f"  {name}: {r.count()} category rows")

print("\n" + "=" * 60)
print("How you'd run it on the cluster")
print("=" * 60)
print(r"""
  # STS runs as a long-lived driver pod (client mode) on Kubernetes, fronted by a Service.
  start:  $SPARK_HOME/sbin/start-thriftserver.sh \
            --master k8s://https://<api-server>:6443 --deploy-mode client \
            --hiveconf hive.server2.thrift.port=10000 \
            --conf spark.kubernetes.namespace=spark-jobs \
            --conf spark.kubernetes.container.image=<registry>/spark:3.5.1 \
            --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
            --conf spark.driver.host=spark-thrift.spark-jobs.svc.cluster.local \
            --conf spark.sql.thriftServer.incrementalCollect=true \
            --conf spark.dynamicAllocation.enabled=true \
            --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
            --conf spark.scheduler.mode=FAIR
  connect: beeline -u 'jdbc:hive2://sts-host:10000'
  Superset/DBT connect to the SAME endpoint (Days 36-37).
""")
print("Analysis Questions")
print("1. Why is the STS driver the shared bottleneck?")
print("2. What does incrementalCollect=true protect against?")
print("3. How do fair-scheduler pools isolate BI from heavy analytics?")

spark.stop()
