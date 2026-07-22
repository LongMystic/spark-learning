# Day 37: Superset Query Optimization

## 🎯 Learning Objectives
- Understand how Superset queries hit Spark (via the Thrift Server)
- Make dashboards fast without overloading the shared cluster
- Design pre-aggregated tables that BI tools love
- Use caching and query limits appropriately

## 📚 Core Concepts

### 1. The path a Superset chart takes
```
Superset chart  --SQLAlchemy/JDBC-->  Spark Thrift Server  -->  YARN executors  -->  Iceberg/Hive
```
Every dashboard tile is a SQL query against the **shared** STS driver. A dashboard with 20 tiles = 20 concurrent Spark queries. Design accordingly.

### 2. The core rule: don't query raw facts from BI
Interactive BI should read **small, pre-aggregated marts**, not billion-row fact tables. Push the heavy aggregation into scheduled Spark/dbt jobs (Days 35–36); let Superset read the compact result.

```
raw transactions (1B rows)  --nightly dbt/Spark-->  fct_daily_sales (100K rows)  <-- Superset reads this
```

### 3. Superset-side levers
- **Query limits** (`Row limit`) so a chart can't pull millions of rows to the driver.
- **Caching** (results cache / thumbnail cache; a Redis/Memcached cache backend) so repeated views don't re-hit Spark.
- **Async queries** (Celery workers) so long queries don't block the web tier.
- **Database timeouts** to kill runaway queries.

## 🔍 Deep Dive: Designing a BI-friendly mart
- **Pre-aggregate** to the dashboard's grain (day × category × region), not raw events.
- **Partition** the mart by the common filter (date) for pruning (Day 5).
- **Broadcast-size dimensions** so joins in ad-hoc exploration stay cheap (Day 25).
- **Keep it small enough to cache** — a few hundred MB reads instantly.
- **Compact** (Iceberg, Day 34) so the mart isn't thousands of small files.

## 💡 Key Insights for On-Premise
### 1. Protect the shared Thrift Server
Superset is a multi-user front-end to one STS driver. Apply Day 29 tuning: incremental collect, fair-scheduler pools (a "BI" pool separate from "analytics"), row limits, and query timeouts. One `SELECT *` from a curious analyst shouldn't freeze the CEO's dashboard.

### 2. Cache invalidation follows the ETL
Set cache TTLs to match data freshness — no point caching for 24h if the mart refreshes hourly, and no point re-querying every second for a daily table. Align Superset cache TTL with the Airflow schedule.

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

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. STS concurrent queries during dashboard peak load.
2. Cache hit rate (Superset).
3. Slowest dashboard queries (SQL Lab / query logs).

### Spark UI Analysis
- STS SQL tab: identify the heaviest recurring BI query → candidate for a pre-agg mart.

## 🚨 Common Issues & Solutions

### Issue 1: Dashboard slow / times out
**Symptom**: tiles spin, queries scan huge facts.
**Solution**: pre-aggregate into a mart, partition by date, enable caching + row limits.

### Issue 2: BI load destabilizes the cluster
**Symptom**: dashboards contend with ETL.
**Solution**: dedicated STS/fair pool for BI; async query workers; timeouts.

## 📝 Key Takeaways
1. Every Superset tile is a Spark query on the shared STS.
2. BI reads small pre-aggregated marts, never raw facts.
3. Partition + compact + broadcast dims to keep marts fast.
4. Use Superset caching, row limits, timeouts, async workers.
5. Isolate BI from ETL (pools/instances) and align cache TTL to ETL.

## 🔗 Next Steps
- **Day 38**: Large-Scale ETL & CDC Patterns

## 📚 Additional Resources
- Superset caching & async query docs; Spark Thrift Server (Day 29)

---

**Progress**: Day 37/40 ✅
