# Day 37: Superset Query Optimization

## 🎯 Learning Objectives
- Understand how Superset queries hit Spark (via the Thrift Server)
- Make dashboards fast without overloading the shared cluster
- Design pre-aggregated tables that BI tools love
- Configure Superset's caching, async query, and row-limit levers correctly
- Diagnose which recurring dashboard query is worth turning into a mart

## 📚 Core Concepts

### 1. The path a Superset chart takes

```
Superset chart  --SQLAlchemy/JDBC-->  Spark Thrift Server  -->  Kubernetes (driver + executor pods)  -->  Iceberg/Hive
```

**Key Points:**
- Every dashboard tile is a SQL query against the **shared** STS driver (a long-lived driver pod, Day 29). A dashboard with 20 tiles = 20 concurrent Spark queries. Design accordingly.
- Superset connects to Spark through a SQLAlchemy dialect (`pyhive`/`hive` style) over the same HiveServer2-compatible JDBC/Thrift endpoint dbt uses (Day 36) — so both tools share one driver's planning and result-collection capacity unless you split them onto separate STS instances.
- A single dashboard "refresh" fans out into as many concurrent SQL statements as it has charts; Superset does not automatically serialize them, so peak concurrency against the STS driver is roughly `(dashboard viewers) x (charts per dashboard)`.

**Example:**
```sql
-- what one Superset chart actually sends over JDBC, generated from its chart config
SELECT category, SUM(amount) AS total
FROM analytics.fct_daily_sales
WHERE txn_date BETWEEN '2026-07-01' AND '2026-07-30'
GROUP BY category
ORDER BY total DESC
LIMIT 100
```

### 2. The core rule: don't query raw facts from BI

Interactive BI should read **small, pre-aggregated marts**, not billion-row fact tables. Push the heavy aggregation into scheduled Spark/dbt jobs (Days 35-36); let Superset read the compact result.

```
raw transactions (1B rows)  --nightly dbt/Spark-->  fct_daily_sales (100K rows)  <-- Superset reads this
```

**Key Points:**
- A mart pre-pays the aggregation cost **once**, offline, on a schedule you control; a raw-fact dashboard pays a full (or partially-pruned) scan cost **every time anyone opens a tab**.
- The right grain for a mart is "the coarsest grain that still answers the dashboard's filters/drill-downs" — usually day x a couple of dimensions, not the finest event grain.
- If analysts need genuine ad-hoc, unpredictable slicing, give them **SQL Lab** (Superset's ad-hoc query workspace) against a separate, lower-priority STS pool — don't let that traffic land on the same dashboards used by executives.

### 3. Superset-side levers

**Key Points:**
- **Query limits** (`Row limit`, and the database-level `SQL_MAX_ROW` setting) so a chart can't pull millions of rows to the driver.
- **Caching** (results cache / thumbnail cache; a Redis/Memcached cache backend configured as `CACHE_CONFIG`/`DATA_CACHE_CONFIG` in `superset_config.py`) so repeated views don't re-hit Spark.
- **Async queries** (a Celery worker pool + a message broker) so long queries run in the background and don't block the Superset web tier (the "Run Async" toggle on a database connection).
- **Database timeouts** (`SQLLAB_TIMEOUT`, `SUPERSET_WEBSERVER_TIMEOUT`, and the connection's own query timeout) to kill runaway queries before they camp on the STS driver.

**Example (`superset_config.py` sketch):**
```python
CACHE_CONFIG = {
    "CACHE_TYPE": "RedisCache",
    "CACHE_DEFAULT_TIMEOUT": 60 * 60,      # 1h default; override per-chart/per-dataset
    "CACHE_KEY_PREFIX": "superset_results",
    "CACHE_REDIS_URL": "redis://redis:6379/1",
}
DATA_CACHE_CONFIG = CACHE_CONFIG          # cache for chart-level result sets
SQLLAB_TIMEOUT = 60 * 5                    # SQL Lab ad-hoc queries: 5 min max
SUPERSET_WEBSERVER_TIMEOUT = 60
RESULTS_BACKEND = ...                      # required for async query result retrieval
```

### 4. Physical tables vs virtual datasets

**Key Points:**
- A Superset **dataset** can point at a physical table/view (`analytics.fct_daily_sales`) or be a **virtual dataset** — a saved `SELECT` that Superset treats like a table. Virtual datasets are convenient for quick exploration but re-run their full `SELECT` as a subquery underneath every chart's generated SQL, adding a query-planning layer the STS has to optimize through.
- Prefer physical marts (a real Iceberg/Hive table built by dbt/Spark, Day 36) for anything used on a recurring, multi-viewer dashboard; reserve virtual datasets for one-off exploration in SQL Lab.
- Column-level metadata (marking a column as a dimension, metric, or temporal column) in the dataset definition determines what Superset can push down as a `GROUP BY`/`WHERE` versus compute client-side — misconfigured column types are a common, invisible source of "why is this simple chart doing a full table scan."

**Example:**
```sql
-- Virtual dataset (a saved query) — re-executed as a subquery on every chart render
SELECT txn_date, category, SUM(amount) AS total
FROM raw.transactions
GROUP BY txn_date, category
-- becomes, per chart:
SELECT category, SUM(total) FROM (  <virtual dataset SQL above>  ) vds
WHERE txn_date BETWEEN :start AND :end GROUP BY category
-- vs a physical mart dataset, where the chart's SQL IS the whole query — no subquery layer:
SELECT category, SUM(total) FROM analytics.fct_daily_sales
WHERE txn_date BETWEEN :start AND :end GROUP BY category
```

## 🔍 Deep Dive: Designing a BI-friendly mart

### Step-by-Step Process

1. **Identify the dashboard's actual grain** — look at every filter and group-by used across its charts (date range, category, region, status) and take the coarsest common denominator.
2. **Build the mart at that grain** on a schedule (nightly dbt `incremental` model, Day 36, or a scheduled Spark job, Day 35) — not on-demand.
3. **Partition** the mart by the column every chart filters on (almost always date) so Superset's date-range filter gets partition pruning for free (Day 5).
4. **Keep dimension tables broadcast-sized** so any ad-hoc join in SQL Lab stays a broadcast join, not a shuffle join (Day 25).
5. **Compact** the mart (Iceberg `rewrite_data_files`, Day 34) so it isn't thousands of small files — small marts can still suffer the small-file problem if written by many small incremental appends.
6. **Set the Superset cache TTL** to match the mart's actual refresh cadence (below).

### Example: From raw fact to dashboard-ready mart

```sql
-- Bad: Superset chart aggregates 1B raw rows on every page load
SELECT category, region, SUM(amount) AS total
FROM raw.transactions                 -- 1B rows, no relevant partition filter shape
WHERE txn_date BETWEEN :start AND :end
GROUP BY category, region

-- Good: a dbt incremental model builds the mart nightly (Day 36)
{{ config(materialized='incremental', incremental_strategy='merge',
          unique_key=['txn_date','category','region'], file_format='iceberg') }}
SELECT txn_date, category, region, SUM(amount) AS total, COUNT(*) AS n
FROM {{ ref('stg_transactions') }}
{% if is_incremental() %} WHERE txn_date >= (SELECT MAX(txn_date) FROM {{ this }}) {% endif %}
GROUP BY txn_date, category, region

-- Superset chart now reads the ~100K-row mart, partition-pruned by its date filter
SELECT category, region, SUM(total) AS total
FROM analytics.fct_daily_sales
WHERE txn_date BETWEEN :start AND :end
GROUP BY category, region
```

**Analysis:**
- The mart query scans a few partitions of a small table instead of the whole fact table — the same partition-pruning mechanics from Day 5, just applied at the BI layer.
- Because the mart is already grouped by `(txn_date, category, region)`, Superset's chart-level `GROUP BY category, region` is a cheap re-aggregation over rows that are already mostly collapsed — this is why picking the right grain in Step 1 matters more than any Superset setting.
- This mart-building job is itself just another scheduled Spark/dbt pipeline (Days 35-36, 38) — BI acceleration is an ETL problem, not a Superset-configuration problem, most of the time.

### Worked example: the concrete cost difference

```
Raw fact:  1,000,000,000 rows, partitioned by txn_date, ~500 partitions (2 years daily)
Dashboard: "last 30 days, by category" -> reads 30 partitions x ~2M rows = ~60M rows scanned,
           THEN groups down to ~150 output rows (30 days x 5 categories).

Mart:      fct_daily_sales, grain (txn_date, category, region) -> ~150K rows total.
Dashboard: same "last 30 days, by category" -> reads 30 partitions x ~500 rows = ~15K rows scanned,
           groups down to the same ~150 output rows.

Same chart, same result: ~60,000,000 rows scanned (raw) vs ~15,000 rows scanned (mart)
-> roughly 4,000x less data read for an identical dashboard tile, before caching even applies.
```

This is why "pre-aggregate, don't optimize the query" is the default answer on Day 37: no amount of STS tuning closes a 4,000x gap in bytes scanned.

### Example: Wiring a dashboard to a fair-scheduler pool

Superset's database connection can pass extra Spark/Hive session properties on connect, which is how a "BI" dashboard database gets pinned to its own fair-scheduler pool (Day 29) separate from the one dbt (Day 36) or SQL Lab uses:

```python
# In the Superset database connection's "Other" -> Engine Parameters, or via SQLAlchemy URI query params:
# jdbc:hive2://sts-host:10000/analytics;spark.sql.thriftserver.scheduler.pool=bi_pool

# fairscheduler.xml on the STS side (Day 29) defines the pool's guaranteed share:
"""
<pool name="bi_pool">
  <schedulingMode>FAIR</schedulingMode>
  <weight>2</weight>
  <minShare>4</minShare>
</pool>
<pool name="sql_lab_pool">
  <schedulingMode>FAIR</schedulingMode>
  <weight>1</weight>
  <minShare>1</minShare>
</pool>
"""
```
Two Superset database connections (one for dashboards, one for SQL Lab) pointed at two different pools on the same STS gives isolation without standing up a second Thrift Server — a lighter-weight first step than fully separate STS instances, worth trying before reaching for more infrastructure.

## 💡 Key Insights for On-Premise

### 1. Protect the shared Thrift Server
Superset is a multi-user front-end to one STS driver. Apply Day 29 tuning: incremental collect, fair-scheduler pools (a "BI" pool separate from "analytics"), row limits, and query timeouts. One `SELECT *` from a curious analyst shouldn't freeze the CEO's dashboard.

### 2. Cache invalidation follows the ETL
Set cache TTLs to match data freshness — no point caching for 24h if the mart refreshes hourly, and no point re-querying every second for a daily table. Align Superset cache TTL with the Airflow schedule (Day 35) that refreshes the underlying mart; a common pattern is to have the last task in the refresh DAG call Superset's cache-invalidation API for the affected dashboards, rather than relying purely on a TTL guess.

### 3. Separate BI traffic from ETL/analytics traffic
Run a dedicated STS (or at minimum a dedicated fair-scheduler pool, Day 29) for Superset, distinct from the one dbt (Day 36) or ad-hoc analysts use. On a shared on-prem cluster, dashboard responsiveness is a user-facing SLA in a way that a batch ETL delay usually isn't — isolate accordingly.

### 4. Thumbnail/snapshot rendering has its own cost
Superset periodically renders dashboard thumbnails and scheduled report snapshots (email/Slack reports); each render re-executes every chart's query. Schedule these for off-peak windows and make sure they hit cache, or they silently multiply STS load at a fixed time every day.

### 5. SQL Lab is a different risk profile than dashboards
Dashboards run known, reviewed queries against marts; SQL Lab lets any analyst run arbitrary SQL against anything they have access to, including raw fact tables. Give SQL Lab its own row limits, timeout, and (ideally) fair-scheduler pool so an exploratory query can't have the same blast radius as a dashboard query.

## 🎯 Practical Exercises

### Exercise 1: Build a BI mart from the sample data
```python
# See exercises/production/exercise-37-superset-mart.py
# Aggregate transactions to a day x category mart; compare query cost vs querying raw.
```

### Exercise 2: Raw vs mart cost
```python
# Time/plan the same "dashboard" query against raw transactions vs the pre-aggregated mart.
```

### Exercise 3: Design a caching/isolation policy
```python
# Extend exercise 37: given a dashboard that refreshes hourly and one that refreshes daily,
# write out (as comments/print statements) the cache TTL you'd set for each, which
# fair-scheduler pool each should use, and the row limit you'd apply to SQL Lab vs dashboards.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **STS concurrent queries during dashboard peak load** — correlate with dashboard view logs to size the fair-scheduler pool.
2. **Cache hit rate** (Superset's results/data cache) — a low hit rate on a rarely-changing mart means the TTL is set too short.
3. **Slowest dashboard queries** (SQL Lab query history / Superset's query log) — recurring slow queries are candidates for a new pre-agg mart.
4. **Async queue depth** (Celery) — a growing backlog means the worker pool is undersized for current BI concurrency.
5. **Row-limit hits** — charts consistently hitting the row limit may be silently truncating results the user doesn't realize are incomplete.

### Spark UI Analysis
- **STS SQL tab**: identify the heaviest recurring BI query → candidate for a pre-agg mart. Look specifically for the same query shape (same `GROUP BY`/filter pattern) appearing repeatedly — that repetition is the signal a mart would pay for itself.
- **Jobs/Stages tabs**: a dashboard query that triggers a full shuffle join against a large dimension table (instead of a broadcast join, Day 25) is a sign the dimension table has grown past the broadcast threshold and needs either a smaller conformed dimension or an explicit broadcast hint.

## 🚨 Common Issues & Solutions

### Issue 1: Dashboard slow / times out
**Symptom**: tiles spin, queries scan huge facts.
**Root Cause**: the dashboard queries a raw or lightly-filtered fact table directly instead of a pre-aggregated mart, so every page view re-pays a full scan/shuffle.
**Solution**: pre-aggregate into a mart, partition by date, enable caching + row limits.

### Issue 2: BI load destabilizes the cluster
**Symptom**: dashboards contend with ETL; both slow down at the same time of day.
**Root Cause**: Superset and heavy batch/analytics workloads share one STS/fair-scheduler pool with no capacity separation.
**Solution**: dedicated STS/fair pool for BI; async query workers; timeouts.

### Issue 3: Cache never seems to help
**Symptom**: cache hit rate stays near zero despite an unchanged dashboard.
**Root Cause**: cache keys are typically a hash of the exact SQL + parameters; dashboards with dynamic filters (e.g. a "last N days" relative filter evaluated at request time) generate a unique cache key on every view, defeating the cache.
**Solution**: bucket relative-time filters to a coarser grain (e.g. cache per-day, not per-second), or accept a short TTL for highly dynamic filters and rely on the mart being small enough that a cache miss is still cheap.

### Issue 4: One analyst's SQL Lab query freezes everyone's dashboards
**Symptom**: dashboard queries queue up or time out right after someone runs an unbounded `SELECT` in SQL Lab.
**Root Cause**: SQL Lab and dashboards share the same STS driver/pool with no isolation, so a single expensive ad-hoc query occupies driver threads and executor capacity that dashboards need.
**Solution**: put SQL Lab on its own fair-scheduler pool (or a separate STS), enforce `SQLLAB_TIMEOUT` and a row limit, and consider requiring `LIMIT` via a linting/CI check on saved queries.

### Issue 5: Thumbnail/report rendering causes a daily spike
**Symptom**: STS load spikes at a fixed time every day, correlated with scheduled email reports or dashboard thumbnail refresh.
**Root Cause**: Superset re-executes every chart query to render thumbnails/snapshots, often scheduled at the top of the hour alongside other cron-like jobs.
**Solution**: stagger thumbnail/report schedules away from ETL windows and other BI peak times, and make sure the underlying marts are cached so re-renders hit cache instead of Spark.

### Issue 6: A simple-looking chart triggers a full table scan
**Symptom**: a single-metric chart against a partitioned mart is unexpectedly slow, with no obvious join or heavy aggregation.
**Root Cause**: the dataset is a virtual dataset (a saved `SELECT` without a partition-column filter baked in), or the dataset's temporal column isn't marked correctly, so Superset's date-range filter never becomes a `WHERE` predicate on the actual partition column — it gets applied to an already-materialized subquery result instead.
**Solution**: convert frequently-used virtual datasets to physical marts, and verify the dataset's column metadata correctly flags the partition/date column as the temporal column Superset filters on.

## 📝 Key Takeaways
1. Every Superset tile is a Spark query on the shared STS.
2. BI reads small pre-aggregated marts, never raw facts.
3. Partition + compact + broadcast dims to keep marts fast.
4. Use Superset caching, row limits, timeouts, and async query workers deliberately, not as an afterthought.
5. Isolate BI from ETL/analytics (separate STS instances or fair pools) and align cache TTL to the ETL refresh schedule.
6. Give SQL Lab (ad-hoc) a different risk profile and capacity lane than reviewed dashboards.
7. A recurring slow dashboard query is a signal to build a mart, not to just raise a timeout.
8. Prefer physical mart datasets over virtual (saved-query) datasets for recurring dashboards; check column metadata (temporal/dimension/metric) so filters push down correctly.

## 🔗 Next Steps
- **Day 38**: Large-Scale ETL & CDC Patterns

## 📚 Additional Resources
- Superset caching & async query docs (`CACHE_CONFIG`, Celery workers)
- Superset database connection / SQLAlchemy dialect docs for Spark/Hive
- Spark Thrift Server (Day 29) fair-scheduler pools

---

**Progress**: Day 37/40 ✅
