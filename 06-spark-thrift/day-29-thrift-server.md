# Day 29: Spark Thrift Server — Architecture & Tuning

## 🎯 Learning Objectives
- Understand what the Spark Thrift Server (STS) is, and how it relates to HiveServer2
- Configure it for multi-user BI access (Superset/DBT/JDBC/ODBC)
- Tune it for concurrency, session isolation, and stability on-premise
- Diagnose the common multi-tenant problems: driver bottlenecks, runaway queries, OOM
- Understand the operational trade-offs of running one always-on Spark application for many users

## 📚 Core Concepts

### 1. What STS is

The Spark Thrift Server is a **long-running Spark application** that exposes a
**HiveServer2-compatible JDBC/ODBC endpoint**. It is literally Spark's
reimplementation of Hive's `HiveServer2` (the class is `HiveThriftServer2`), so
any client that already knows how to speak the Hive/Thrift wire protocol
(`beeline`, most BI tools, ODBC drivers, DBT's `spark`/`hive` adapters) can
connect to it without any Spark-specific client library.

BI tools (Superset), SQL clients (`beeline`), and DBT connect to it and run SQL
that executes on Spark — sharing **one** SparkContext and its executors across
all users.

```
Superset / beeline / DBT  --JDBC(10000)-->  Thrift Server (driver pod + shared SparkContext)  -->  executor pods (Kubernetes)
```

**Key Points:**
- STS speaks the **Thrift** RPC protocol used by HiveServer2 — the JDBC driver class is `org.apache.hive.jdbc.HiveDriver`, connection string `jdbc:hive2://host:10000/db`.
- Every SQL statement submitted over JDBC becomes a normal Spark SQL job — it goes through the same Catalyst/AQE pipeline as `spark-submit` batch jobs.
- Because it is one process, all connected users **share the executor pool, the SQL cache, and the catalog** — a query from user A can benefit from data user B already cached.
- It is the natural front door for **Hive/Iceberg tables** registered in a metastore, which is why it pairs with Days 33-34 (Iceberg) and Day 37 (Superset/DBT).

**Example — connecting with beeline:**
```bash
beeline -u "jdbc:hive2://spark-thrift.spark-jobs.svc.cluster.local:10000" \
  -n spark_user
> SHOW DATABASES;
> USE db;
> SELECT COUNT(*) FROM transactions;
```

### 2. STS vs a fresh spark-submit

| | Thrift Server | Per-job spark-submit |
|---|---|---|
| Startup | already running (no per-query JVM start) | pays startup each job |
| Sharing | one SparkContext, shared cache & executors | isolated |
| Best for | interactive BI, many small/medium queries | heavy batch ETL |
| Risk | one bad query can hurt everyone | isolated blast radius |
| Lifecycle | managed like a service (`Deployment`) | managed like a batch job (`SparkApplication`) |

**Key Points:**
- Choose STS when **latency matters more than isolation** — dashboards, ad-hoc SQL, DBT models running on a schedule against live data.
- Choose per-job `spark-submit` (or a `SparkApplication` CR via the Spark Operator, Day 17) when a job needs **guaranteed resources and blast-radius isolation** — nightly ETL, heavy CDC merges (Day 38).
- A cluster commonly runs **both**: STS for interactive/BI, and separate `SparkApplication`s for batch, so they don't compete.

### 3. Starting it

The STS is a **long-running driver** — on Kubernetes it runs as a **driver pod**
(managed by a `Deployment`/`StatefulSet`) in **client mode**, so the JDBC
endpoint stays put while executor pods come and go. A **headless Service**
gives the driver a stable DNS name so executor pods can reach it back
(`spark.driver.host`).

**Key Points:**
- Client mode is deliberate: the driver *is* the JDBC server — it must be a pod you manage and expose via a `Service`, not a transient pod the operator schedules and tears down like a batch job.
- Config flows through `--hiveconf` (HiveServer2-level settings like `hive.server2.thrift.port`) and `--conf` (Spark-level settings).
- The pod needs a `serviceAccount` with RBAC to launch executor pods in its namespace (same pattern as Day 17's Spark Operator RBAC).

**Example:**
```bash
# Runs inside the driver pod (its container command):
$SPARK_HOME/sbin/start-thriftserver.sh \
  --master k8s://https://<api-server>:6443 --deploy-mode client \
  --hiveconf hive.server2.thrift.port=10000 \
  --hiveconf hive.server2.thrift.min.worker.threads=5 \
  --hiveconf hive.server2.thrift.max.worker.threads=200 \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.container.image=<registry>/spark:3.5.1 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.driver.host=spark-thrift.spark-jobs.svc.cluster.local \
  --conf spark.driver.port=7078 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.dynamicAllocation.enabled=true \
  --conf spark.dynamicAllocation.shuffleTracking.enabled=true \
  --conf spark.dynamicAllocation.maxExecutors=50 \
  --conf spark.sql.thriftServer.incrementalCollect=true
# connect:  beeline -u 'jdbc:hive2://sts-host:10000'
```
> Client mode (not cluster mode) is deliberate: you want the driver — which *is*
> the JDBC server — to be the pod you manage and expose via a Service, and it
> launches executor pods itself.

### 4. Sessions, isolation, and `singleSession`

**Key Points:**
- Each JDBC/ODBC connection gets its own **session** with its own temp views and `SET` configs by default — but they all still run on the **same SparkContext/executors**.
- `spark.sql.hive.thriftServer.singleSession` (default `false`): if set to `true`, **all** connections share one `SparkSession` — useful if you want a temp view created by one user visible to another, but it means SQL configs (`SET spark.sql.shuffle.partitions=...`) leak across users too. Leave it `false` for multi-tenant BI so users don't stomp on each other's session state.
- `hive.server2.thrift.min.worker.threads` / `hive.server2.thrift.max.worker.threads` size the thread pool that serves concurrent JDBC connections — too low and connections queue up waiting for a worker thread, independent of Spark's own job scheduling.

**Example:**
```sql
-- session A
SET spark.sql.shuffle.partitions=8;
CREATE TEMP VIEW recent AS SELECT * FROM transactions WHERE txn_date >= current_date() - 7;

-- session B (default singleSession=false): does NOT see `recent`, has its own shuffle.partitions
```

### 5. Security and access on-premise

**Key Points:**
- STS inherits HiveServer2's authentication hooks — `hive.server2.authentication` can be set to `NONE` (trust-based, fine for an internal cluster network), `LDAP`, or `KERBEROS` depending on what identity system the rest of the on-prem platform already uses.
- Authorization (which tables/rows a user may see) is layered on **top** of STS, typically via the metastore/catalog's own permission model or a policy layer (e.g. Ranger-style plugins in some Hadoop-derived stacks) rather than something STS enforces itself out of the box.
- Because STS shares one SparkContext, there is **no per-user resource isolation** at the JVM level — authentication controls who can connect, but it does not prevent an authenticated user from running a query heavy enough to affect everyone else. That's still a scheduling/pool problem (see Deep Dive).
- For BI tools connecting over the network (not just `localhost`), terminate TLS at the Service/ingress layer or configure `hive.server2.use.SSL` so JDBC traffic isn't sent in plaintext across the cluster network.

**Example:**
```bash
--hiveconf hive.server2.authentication=LDAP
--hiveconf hive.server2.authentication.ldap.url=ldap://ldap.internal:389
--hiveconf hive.server2.use.SSL=true
```

## 🔍 Deep Dive: Tuning for concurrency

### Step-by-Step Process

1. **Size the driver first.** The driver plans every query and (without incremental collect) can buffer entire result sets — give it real memory (`--driver-memory 4g`+) and dedicated CPU, since it's shared by everyone.
2. **Turn on dynamic allocation with shuffle tracking.** Kubernetes has **no external shuffle service**, so `spark.dynamicAllocation.shuffleTracking.enabled=true` is required for executors to be released safely between BI bursts without losing shuffle data mid-query (Day 18).
3. **Guard result size.** Set `spark.sql.thriftServer.incrementalCollect=true` so large result sets stream back to the client instead of materializing fully on the driver, and set `spark.driver.maxResultSize` as a hard backstop.
4. **Isolate workload classes with the FAIR scheduler.** Map connections/users to pools so one heavy analytical query can't starve a dashboard's sub-second query.
5. **Keep AQE and broadcast joins on.** Most BI queries are star-schema joins against small dimension tables (Day 11) — AQE's dynamic broadcast/partition-coalescing keeps them fast without manual tuning per query.
6. **Decide on `singleSession`.** Default (`false`) for isolation; `true` only for trusted, single-team setups that want shared temp views.

### Example: Fair scheduler pools

```xml
<!-- fairscheduler.xml -->
<?xml version="1.0"?>
<allocations>
  <pool name="bi_dashboards">
    <schedulingMode>FAIR</schedulingMode>
    <weight>3</weight>
    <minShare>4</minShare>
  </pool>
  <pool name="adhoc_analytics">
    <schedulingMode>FAIR</schedulingMode>
    <weight>1</weight>
    <minShare>0</minShare>
  </pool>
</allocations>
```
```bash
--conf spark.scheduler.mode=FAIR
--conf spark.scheduler.allocation.file=/etc/spark/fairscheduler.xml
```
```sql
-- from a JDBC session, before running a heavy ad-hoc query:
SET spark.sql.thriftserver.scheduler.pool=adhoc_analytics;
```

**Analysis:**
- `bi_dashboards` gets a guaranteed minimum share (4 tasks worth) and a 3x weight, so dashboard queries keep making progress even while `adhoc_analytics` runs a big scan.
- Pools are a **cooperative** mechanism — a session must `SET ... scheduler.pool` to opt in; there's no way to force a rogue client into a pool from the server side, which is why process-level isolation (separate STS instances) is sometimes simpler than perfecting pool config.
- Combined with `incrementalCollect` and `maxResultSize`, pools address the two distinct failure modes: **CPU/executor contention** (pools) and **driver memory exhaustion** (result guards).

## 💡 Key Insights for On-Premise

### 1. The driver is the shared bottleneck
Every query's planning and result collection happens in the **single STS
driver**. Give it ample memory (`--driver-memory`), enable incremental
collect, and stop users from `SELECT *`-ing huge tables to the driver.

### 2. One STS per workload class
Run a **separate** Thrift Server for interactive dashboards vs. heavy ad-hoc
analytics, so a runaway analytics query can't freeze executive dashboards.
Isolation by process is simpler than perfect fair-scheduler tuning.

### 3. HA & restarts
STS is a single JVM — if it dies, all sessions drop. Run the driver pod under
a **Deployment/StatefulSet** so Kubernetes restarts it automatically, expose
it through a **Service**, and consider a load-balanced pair of STS pods for HA.

### 4. No external shuffle service changes the allocation story
On YARN, dynamic allocation could safely release executors mid-query because
the external shuffle service kept serving their shuffle blocks. On
Kubernetes there is no such service by default, so `shuffleTracking.enabled`
is what keeps an executor alive until its shuffle output is no longer
needed — without it, aggressive scale-down under BI load can silently lose
shuffle data and force expensive recomputation.

## 🎯 Practical Exercises

### Exercise 1: Local Thrift-style access
```python
# See exercises/production/exercise-29-thrift-server.py
# Simulates the shared-context / per-session model with spark.newSession():
#   - one shared SparkContext + a global temp view (like STS's shared catalog)
#   - two sessions set different spark.sql.shuffle.partitions, proving per-session
#     SQL config isolation while executors stay shared
#   - prints the actual start-thriftserver.sh / beeline commands you'd use on the
#     cluster, including FAIR scheduler mode and incrementalCollect
s1 = spark.newSession()
s2 = spark.newSession()
s1.conf.set("spark.sql.shuffle.partitions", "8")
s2.conf.set("spark.sql.shuffle.partitions", "64")
```

### Exercise 2: Fair-scheduler pools
```python
# Assign queries to pools (bi_dashboards vs adhoc_analytics) and reason about
# isolation. Try: run a big shuffle-heavy job in "adhoc_analytics" while
# running small aggregations in "bi_dashboards"; observe in the Spark UI's
# Stages tab that both pools get scheduled fairly instead of FIFO.
spark.sparkContext.setLocalProperty("spark.scheduler.pool", "bi_dashboards")
df.groupBy("category").count().show()

spark.sparkContext.setLocalProperty("spark.scheduler.pool", "adhoc_analytics")
df.groupBy("customer_id").sum("amount").show()
```

### Exercise 3: Reason about security configuration
```text
# No code required — design exercise:
# 1. Your cluster already has LDAP for user identity. Sketch the hiveconf
#    flags you'd add to start-thriftserver.sh to require LDAP auth over TLS.
# 2. Given STS has no per-user resource isolation, what combination of FAIR
#    pools + separate STS instances would you use to guarantee the exec
#    dashboard team never waits on the data-science team's queries?
# 3. If `singleSession=true` were enabled for convenience (shared temp views),
#    what SQL-config leakage risk would that introduce between users?
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **STS driver memory & GC** — the shared bottleneck; watch for old-gen pressure as concurrent sessions grow.
2. **Active sessions & concurrent statements** — via the JDBC/ODBC tab; a growing backlog means the worker thread pool or executors are saturated.
3. **Executor count over the day** — confirms dynamic allocation is scaling with the BI traffic pattern (up during business hours, down overnight).
4. **Per-query duration and result size** — long tails usually mean a missing filter/partition prune or a `SELECT *` heading to the driver.
5. **Scheduler pool utilization** — if using FAIR pools, confirm `minShare` is actually being honored under load.

### Spark UI Analysis
- The STS has its own Spark UI (port 4040+); the **JDBC/ODBC tab** shows sessions, connected users, and per-statement SQL text with start/close times — the first place to look when a user reports "my query is stuck."
- The **SQL tab** shows the physical plan for each statement — check for broadcast joins on dimension tables and partition pruning on fact tables (Days 11-12).
- The **Executors tab** shows per-executor storage memory and active tasks — useful to spot one heavy query monopolizing executors that dashboards need.
- The **Stages tab**, when FAIR scheduling is enabled, breaks stages out by pool so you can visually confirm isolation is working.

## 🚨 Common Issues & Solutions

### Issue 1: Driver OOM on a big SELECT
**Symptom**: STS crashes, all sessions drop simultaneously.
**Root Cause**: A client ran `SELECT *` (or an unfiltered aggregation) against a large table; without incremental collection, Spark buffers the entire result set in the driver before sending it over Thrift.
**Solution**:
```bash
--conf spark.sql.thriftServer.incrementalCollect=true
--conf spark.driver.maxResultSize=2g
```
Also enforce row limits at the BI layer (Superset query limits) so a mistaken dashboard query can't reach the driver at all.

### Issue 2: One user's query freezes dashboards
**Symptom**: Dashboard queries that normally return in under a second start timing out while an analyst runs an ad-hoc scan.
**Root Cause**: Default FIFO scheduling — all queries share the executor pool with no fairness guarantee, so a long-running job can occupy every executor slot.
**Solution**: Enable FAIR scheduling with pools (see Deep Dive), or run a **separate STS instance** for the analytics workload class entirely.

### Issue 3: JDBC connections queue up / time out
**Symptom**: New `beeline`/BI connections hang before even running a query.
**Root Cause**: `hive.server2.thrift.max.worker.threads` is too low for the number of concurrent connections (each connection holds a worker thread for its lifetime).
**Solution**:
```bash
--hiveconf hive.server2.thrift.min.worker.threads=10
--hiveconf hive.server2.thrift.max.worker.threads=500
```
Also close idle connections from the BI tool side (connection pool timeouts) so threads are recycled.

### Issue 4: Temp view from one user "disappears" for another
**Symptom**: A `CREATE TEMP VIEW` visible in one BI session isn't visible in another.
**Root Cause**: This is expected — with `spark.sql.hive.thriftServer.singleSession=false` (the default), each JDBC connection has an isolated session and temp views don't cross sessions.
**Solution**: Use a **global temp view** (`CREATE GLOBAL TEMPORARY VIEW`, queried as `global_temp.viewname`) if it truly needs cross-session visibility, or register it as a real table/view in the metastore instead of relying on `singleSession=true` (which removes session isolation for everyone).

### Issue 5: STS restart drops all in-flight queries
**Symptom**: A pod restart (OOM, node drain, rolling upgrade) kills every active dashboard query with no warning to users.
**Root Cause**: STS holds all session and query state in a single JVM; Kubernetes restarting the pod is a clean process kill from Spark's point of view.
**Solution**: Run at least two STS replicas behind a load-balanced Service for HA, keep driver memory comfortably under its limit to avoid OOM-triggered restarts, and schedule rolling restarts for low-traffic windows.

## 📝 Key Takeaways
1. STS is a long-lived Spark app exposing HiveServer2-compatible JDBC/ODBC for BI/DBT.
2. One shared SparkContext — great for interactivity and shared caching, risky for isolation.
3. Enable dynamic allocation + shuffle tracking (no external shuffle service on K8s) + AQE.
4. Use `incrementalCollect`, `maxResultSize`, and worker-thread sizing to protect the shared driver.
5. `singleSession=false` (default) isolates per-connection state; only flip it for trusted shared-temp-view use cases.
6. Separate STS instances (or FAIR pools) isolate workloads; plan for HA with multiple replicas.

## 🔗 Next Steps
- **Day 30**: Structured Streaming Fundamentals

## 📚 Additional Resources
- Spark "Distributed SQL Engine" (Thrift Server) documentation
- Hive `HiveServer2` configuration reference (`hive.server2.*` properties, inherited by STS)
- Spark Fair Scheduler documentation (pools, scheduling modes)
- `beeline` client usage guide

---

**Progress**: Day 29/40 ✅
