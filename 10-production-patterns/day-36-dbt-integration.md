# Day 36: DBT-on-Spark Integration

## 🎯 Learning Objectives
- Understand how dbt runs SQL transformations on Spark/Thrift
- Choose materializations (view/table/incremental) wisely
- Use incremental models to avoid full rebuilds
- Fit dbt into the on-prem Spark + Iceberg + Airflow stack

## 📚 Core Concepts

### 1. What dbt does here
dbt compiles your **SQL models** into `CREATE TABLE/VIEW`/`INSERT` statements and runs them through a warehouse — here, **Spark** via `dbt-spark` (over the Thrift Server or a session). dbt owns *transformation SQL + tests + docs + lineage*; Spark executes it.

```
dbt models (SQL + Jinja)  --dbt-spark-->  Spark Thrift Server  -->  YARN  -->  Hive/Iceberg tables
```

### 2. Connection (`profiles.yml`)
```yaml
my_project:
  target: prod
  outputs:
    prod:
      type: spark
      method: thrift            # connect to the Spark Thrift Server (Day 29)
      host: sts-host
      port: 10000
      schema: analytics
      threads: 4
```

### 3. Materializations
| Type | Produces | Use for |
|------|----------|---------|
| `view` | a view | light transforms, always-fresh |
| `table` | full rebuild each run | small/medium marts |
| `incremental` | append/merge new rows only | large fact tables |

```sql
-- models/fct_daily_sales.sql
{{ config(materialized='incremental', unique_key='txn_date',
          incremental_strategy='merge', file_format='iceberg') }}
SELECT txn_date, category, SUM(amount) AS total
FROM {{ ref('stg_transactions') }}
{% if is_incremental() %}
  WHERE txn_date >= (SELECT MAX(txn_date) FROM {{ this }})
{% endif %}
GROUP BY txn_date, category
```

## 🔍 Deep Dive: Incremental strategies on Spark
- **`append`** — fastest, but risks duplicates on re-run; pair with idempotent filters.
- **`merge`** — atomic upsert (needs Iceberg/Delta) → the on-prem sweet spot (Day 34).
- **`insert_overwrite`** — overwrite matched partitions (dynamic partition overwrite under the hood, Day 21).

Choose based on the sink: Iceberg → `merge`; plain Parquet/Hive partitions → `insert_overwrite`.

## 💡 Key Insights for On-Premise
### 1. dbt runs *through* the Thrift Server — so tune STS
dbt concurrency (`threads`) hits the shared STS driver. Everything from Day 29 (incremental collect, fair pools, dynamic allocation) applies. Too many threads can overwhelm one STS.

### 2. Tests are a first-class quality gate
`not_null`, `unique`, `accepted_values`, and custom tests run as SQL on Spark. Run `dbt test` in Airflow **before** publishing downstream — cheaper than debugging bad marts later (ties to Day 38).

### 3. Let dbt own lineage, Airflow own scheduling
Common pattern: Airflow runs `dbt run`/`dbt test` as tasks; dbt manages the DAG *within* the transformation layer. Don't duplicate model dependencies in Airflow.

## 🎯 Practical Exercises

### Exercise 1: Model the sample data as dbt-style SQL
```python
# See exercises/production/exercise-36-dbt-model.py
# Express a staging model + an incremental fact model as Spark SQL and run them locally
# (simulating what dbt would compile and submit).
```

### Exercise 2: Incremental logic
```python
# Implement the is_incremental() WHERE clause by hand and verify only new dates are processed.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. `dbt run` model timings (which model is slow?).
2. STS driver load during dbt runs (threads too high?).
3. Test pass/fail counts.

## 🚨 Common Issues & Solutions

### Issue 1: Incremental model duplicates rows
**Symptom**: counts grow on re-run.
**Solution**: use `merge` with a `unique_key`, or `insert_overwrite` by partition — not bare `append`.

### Issue 2: dbt overwhelms the Thrift Server
**Symptom**: STS slow/OOM during `dbt run`.
**Solution**: lower `threads`, use fair pools, ensure incremental (not full `table`) for big models.

## 📝 Key Takeaways
1. dbt = SQL transforms + tests + lineage; Spark executes via `dbt-spark`.
2. Connect through the Thrift Server; STS tuning applies.
3. Use `incremental` + `merge` (Iceberg) for large facts — avoid full rebuilds.
4. Run `dbt test` as a pre-publish quality gate.
5. Airflow schedules `dbt run/test`; dbt owns model lineage.

## 🔗 Next Steps
- **Day 37**: Superset Query Optimization

## 📚 Additional Resources
- dbt-spark adapter docs; dbt incremental models

---

**Progress**: Day 36/40 ✅
