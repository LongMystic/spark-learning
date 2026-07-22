# Day 29: Spark Thrift Server — Architecture & Tuning

## 🎯 Learning Objectives
- Understand what the Spark Thrift Server (STS) is and when to use it
- Configure it for multi-user BI access (Superset/DBT/JDBC)
- Tune it for concurrency and stability on-premise
- Diagnose the common multi-tenant problems

## 📚 Core Concepts

### 1. What STS is
The Spark Thrift Server is a **long-running Spark application** that exposes a **HiveServer2-compatible JDBC/ODBC endpoint**. BI tools (Superset), SQL clients (`beeline`), and DBT connect to it and run SQL that executes on Spark — sharing **one** SparkContext and its executors across all users.

```
Superset / beeline / DBT  --JDBC(10000)-->  Thrift Server (driver + shared SparkContext)  -->  YARN executors
```

### 2. STS vs a fresh spark-submit
| | Thrift Server | Per-job spark-submit |
|---|---|---|
| Startup | already running (no per-query JVM start) | pays startup each job |
| Sharing | one SparkContext, shared cache & executors | isolated |
| Best for | interactive BI, many small/medium queries | heavy batch ETL |
| Risk | one bad query can hurt everyone | isolated blast radius |

### 3. Starting it
```bash
$SPARK_HOME/sbin/start-thriftserver.sh \
  --master yarn --deploy-mode client \
  --hiveconf hive.server2.thrift.port=10000 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.maxExecutors=50 \
  --conf spark.sql.thriftServer.incrementalCollect=true
# connect:  beeline -u 'jdbc:hive2://sts-host:10000'
```

## 🔍 Deep Dive: Tuning for concurrency

- **Dynamic allocation + external shuffle service** — so idle BI periods release executors and bursts scale up (Day 18).
- **`spark.sql.thriftServer.incrementalCollect=true`** — streams large result sets back incrementally instead of collecting all to the driver (prevents driver OOM from a `SELECT *`).
- **Scheduler pools (fair scheduler)** — isolate users/teams so one heavy query doesn't starve dashboards:
  ```bash
  --conf spark.scheduler.mode=FAIR
  --conf spark.scheduler.allocation.file=/etc/spark/fairscheduler.xml
  ```
- **Broadcast + AQE on** — most BI queries are star joins; AQE and broadcast keep them snappy.
- **Result size guards** — `spark.driver.maxResultSize`, query timeouts, row limits at the BI layer.

## 💡 Key Insights for On-Premise

### 1. The driver is the shared bottleneck
Every query's planning and result collection happens in the **single STS driver**. Give it ample memory (`--driver-memory`), enable incremental collect, and stop users from `SELECT *`-ing huge tables to the driver.

### 2. One STS per workload class
Run a **separate** Thrift Server for interactive dashboards vs. heavy ad-hoc analytics, so a runaway analytics query can't freeze executive dashboards. Isolation by process is simpler than perfect fair-scheduler tuning.

### 3. HA & restarts
STS is a single JVM — if it dies, all sessions drop. Run it under a supervisor (systemd / Ambari / CM), and consider a load balancer over two instances for HA.

## 🎯 Practical Exercises

### Exercise 1: Local Thrift-style access
```python
# See exercises/production/exercise-29-thrift-server.py
# Simulate the shared-session model and incremental collection locally; learn the
# beeline connection + config you'd use on the cluster.
```

### Exercise 2: Fair-scheduler pools
```python
# Assign queries to pools and reason about isolation between BI and analytics.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. STS driver memory & GC (shared bottleneck).
2. Active sessions & concurrent queries.
3. Executor count over the day (dynamic allocation working?).

### Spark UI Analysis
- The STS has its own Spark UI (port 4040+); the JDBC/ODBC tab shows sessions and per-query SQL.

## 🚨 Common Issues & Solutions

### Issue 1: Driver OOM on a big SELECT
**Symptom**: STS crashes, all sessions drop.
**Solution**: enable `incrementalCollect`, raise driver memory, enforce BI row limits.

### Issue 2: One user's query freezes dashboards
**Symptom**: contention.
**Solution**: fair-scheduler pools, or separate STS instances per workload class.

## 📝 Key Takeaways
1. STS is a long-lived Spark app exposing HiveServer2 JDBC for BI/DBT.
2. One shared SparkContext — great for interactivity, risky for isolation.
3. Enable dynamic allocation + external shuffle service + AQE.
4. Use `incrementalCollect` and result guards to protect the shared driver.
5. Separate STS instances (or fair pools) isolate workloads; plan for HA.

## 🔗 Next Steps
- **Day 30**: Structured Streaming Fundamentals

## 📚 Additional Resources
- Spark "Distributed SQL Engine" (Thrift Server) docs; Fair Scheduler docs

---

**Progress**: Day 29/40 ✅
