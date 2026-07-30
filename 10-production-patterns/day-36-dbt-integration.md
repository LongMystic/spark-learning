# Day 36: DBT-on-Spark Integration

## 🎯 Learning Objectives
- Understand how dbt runs SQL transformations on Spark/Thrift
- Configure the `dbt-spark` adapter and its connection methods
- Choose materializations (view/table/incremental) wisely
- Use incremental models and merge strategies to avoid full rebuilds
- Use dbt tests, sources, and docs as a production quality/lineage layer
- Fit dbt into the on-prem Spark + Iceberg + Airflow stack

## 📚 Core Concepts

### 1. What dbt does here

dbt compiles your **SQL models** into `CREATE TABLE/VIEW`/`INSERT`/`MERGE` statements and runs them through a warehouse — here, **Spark** via `dbt-spark` (over the Thrift Server or a session). dbt owns *transformation SQL + tests + docs + lineage*; Spark executes it.

**Key Points:**
- dbt is not a compute engine — it never moves data itself. It renders Jinja+SQL templates and hands the resulting DDL/DML to Spark to execute.
- Model dependencies (`{{ ref('other_model') }}`) build a DAG automatically; dbt topologically sorts and runs models in the right order, in parallel where the graph allows (`threads`).
- Because dbt models are just SQL files under version control, they get code review, CI, and a searchable lineage graph (`dbt docs generate`) for free — a real gap the raw Spark-job approach (Days 1-34) doesn't fill on its own.

**Example:**
```
dbt models (SQL + Jinja)  --dbt-spark-->  Spark Thrift Server  -->  Kubernetes (driver + executor pods)  -->  Hive/Iceberg tables
```
The Thrift Server itself runs as a long-lived **driver pod** on Kubernetes (Day 29); dbt connects to its JDBC endpoint.

### 2. The `dbt-spark` adapter and connection methods

**Key Points:**
- `dbt-spark` supports three connection methods: `thrift` (JDBC to a running Spark Thrift Server — the standard on-prem shared-cluster pattern), `http` (Databricks-style HTTP endpoint — not applicable here), and `session` (an embedded local Spark session inside the dbt process — useful for local dev/CI, not production).
- On this stack, `method: thrift` is the right choice: it reuses the same STS you tuned on Day 29, and it means dbt never needs its own Kubernetes RBAC identity to launch executor pods — the STS driver already has one.
- `file_format` in a model's config controls the storage/table format dbt creates (`parquet`, `hive`, `iceberg`, `delta`); on this stack, **`iceberg`** is preferred for anything using `incremental_strategy='merge'` (Day 33-34).

**Example (`profiles.yml`):**
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
    dev:
      type: spark
      method: session           # local embedded Spark for fast iteration/CI only
      schema: analytics_dev
      host: NA
```

### 3. Materializations

| Type | Produces | Use for |
|------|----------|---------|
| `view` | a view | light transforms, always-fresh |
| `table` | full rebuild each run | small/medium marts |
| `incremental` | append/merge new rows only | large fact tables |
| `ephemeral` | inlined as a CTE, not materialized | thin reusable SQL snippets, no storage cost |

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

**Explanation:**
- `{{ this }}` refers to the model's own already-materialized table — used inside `is_incremental()` blocks to find the current high-water mark.
- `unique_key` tells the `merge` strategy which column(s) identify "the same row" for upsert purposes — analogous to the `ON` clause of a raw `MERGE INTO` (Day 34).
- A model with no `is_incremental()` guard on an `incremental` materialization still works on the *first* run (dbt detects the target table doesn't exist yet and runs a full build), but every run afterward only processes what the `WHERE` clause lets through.

### 4. Sources, tests, and docs as a governance layer

**Key Points:**
- `sources.yml` declares the upstream raw tables (bronze, Day 38) dbt reads but doesn't create, so lineage graphs show the true origin, and `dbt source freshness` can alert when upstream data goes stale.
- Built-in **generic tests** — `not_null`, `unique`, `accepted_values`, `relationships` — compile to SQL `SELECT` statements that must return zero rows to pass; they run as ordinary Spark queries through the same STS.
- `dbt docs generate` produces a browsable DAG of every model, its columns, and its tests — the closest thing this stack has to a data catalog, without introducing a new tool.

**Example:**
```yaml
# models/staging/sources.yml
sources:
  - name: raw
    tables:
      - name: transactions
        loaded_at_field: _loaded_at
        freshness:
          warn_after: {count: 6, period: hour}
          error_after: {count: 24, period: hour}

# models/marts/schema.yml
models:
  - name: fct_daily_sales
    columns:
      - name: txn_date
        tests: [not_null]
      - name: category
        tests:
          - not_null
          - relationships: {to: ref('dim_category'), field: category_id}
```

### 5. dbt snapshots: SCD Type 2 without hand-written MERGE

**Key Points:**
- A `snapshots/` model captures the state of a mutable source table over time by adding `dbt_valid_from`/`dbt_valid_to`/`dbt_scd_id` columns — dbt's built-in implementation of the SCD Type 2 pattern from Day 38, without writing the `MERGE INTO ... valid_to`/`is_current` logic by hand.
- Two strategies: `timestamp` (compares a source `updated_at` column to detect changes) and `check` (compares an explicit list of columns row-by-row when no reliable `updated_at` exists).
- Snapshots run on their own schedule (`dbt snapshot`, typically before `dbt run`) since they need to capture the *pre-transform* source state — run them too infrequently and you silently lose intermediate history for any row that changed twice between snapshots.

**Example:**
```sql
-- snapshots/customers_snapshot.sql
{% snapshot customers_snapshot %}
{{ config(target_schema='snapshots', unique_key='customer_id',
          strategy='timestamp', updated_at='updated_at') }}
SELECT customer_id, name, email, updated_at FROM {{ source('raw', 'customers') }}
{% endsnapshot %}
-- dbt maintains dbt_valid_from/dbt_valid_to/dbt_scd_id automatically on every `dbt snapshot` run
```

## 🔍 Deep Dive: Incremental strategies on Spark

### Step-by-Step Process

1. **First run**: the target table doesn't exist yet, so dbt ignores the `is_incremental()` guard and runs the model's SQL in full — a normal `CREATE TABLE AS SELECT`.
2. **Subsequent runs**: dbt wraps the model SQL according to `incremental_strategy` and only touches new/changed rows.
3. **`append`** — dbt runs `INSERT INTO target SELECT ...` with the `is_incremental()` filter applied. Fastest, but re-running the *same* dbt run twice (e.g. an Airflow retry) will double-insert unless the filter is itself watermark-safe and non-overlapping.
4. **`merge`** — dbt compiles a `MERGE INTO target USING (model SQL) ON unique_key WHEN MATCHED ... WHEN NOT MATCHED ...` — an atomic upsert. Requires a table format that supports `MERGE` (**Iceberg**, Day 34) — this is the on-prem sweet spot for large, frequently-updated facts.
5. **`insert_overwrite`** — dbt overwrites whole partitions matched by the model's `WHERE`, using Spark's **dynamic partition overwrite** under the hood (Day 21). Works on plain Hive/Parquet partitioned tables without needing Iceberg.

### Example: Choosing a strategy by sink

```sql
-- Iceberg sink: idempotent upsert keyed by natural key -> merge
{{ config(materialized='incremental', incremental_strategy='merge',
          unique_key='txn_id', file_format='iceberg') }}
SELECT * FROM {{ ref('stg_transactions') }}
{% if is_incremental() %} WHERE _loaded_at > (SELECT MAX(_loaded_at) FROM {{ this }}) {% endif %}

-- Plain Hive/Parquet sink: overwrite whole partitions -> insert_overwrite
{{ config(materialized='incremental', incremental_strategy='insert_overwrite',
          partition_by=['txn_date']) }}
SELECT * FROM {{ ref('stg_transactions') }}
{% if is_incremental() %} WHERE txn_date >= '{{ var("run_date") }}' {% endif %}
```

**Analysis:**
- Choose based on the sink: Iceberg → `merge` (row-level correctness, handles late-arriving updates/deletes); plain Parquet/Hive partitions → `insert_overwrite` (partition-level correctness, cheaper when whole days are naturally re-computed).
- `append` should be reserved for genuinely append-only, non-overlapping event streams where duplicate delivery is already handled upstream (e.g. exactly-once Kafka consumption, Day 30-31) — otherwise prefer `merge`/`insert_overwrite` so dbt runs stay safe to retry, matching the idempotency principle from Day 35 and Day 21.

### Worked example: what actually runs on Spark

```sql
-- Compiled by dbt for incremental_strategy='merge' — this is what hits the STS
MERGE INTO analytics.fct_daily_sales AS target
USING (
  SELECT txn_date, category, SUM(amount) AS total
  FROM analytics.stg_transactions
  WHERE txn_date >= (SELECT MAX(txn_date) FROM analytics.fct_daily_sales)
  GROUP BY txn_date, category
) AS source
ON target.txn_date = source.txn_date AND target.category = source.category
WHEN MATCHED THEN UPDATE SET target.total = source.total
WHEN NOT MATCHED THEN INSERT (txn_date, category, total)
  VALUES (source.txn_date, source.category, source.total);
```
This is exactly the raw `MERGE INTO` pattern from Day 34, just generated by dbt instead of hand-written — running `dbt compile` and reading `target/compiled/.../fct_daily_sales.sql` shows the real SQL before it ever reaches the STS, making it easy to review in a pull request.

## 💡 Key Insights for On-Premise

### 1. dbt runs *through* the Thrift Server — so tune STS
dbt concurrency (`threads`) hits the shared STS driver. Everything from Day 29 (incremental collect, fair pools, dynamic allocation) applies. Too many threads can overwhelm one STS — a `dbt run` with `threads: 16` against models that each scan large tables can look, from the STS's perspective, exactly like 16 simultaneous "bad" analyst queries.

### 2. Tests are a first-class quality gate
`not_null`, `unique`, `accepted_values`, and custom (SQL-file) tests run as SQL on Spark. Run `dbt test` in Airflow **before** publishing downstream — cheaper than debugging bad marts later (ties to Day 38). A failed test should fail the Airflow task, not just print a warning.

### 3. Let dbt own lineage, Airflow own scheduling
Common pattern: Airflow runs `dbt run`/`dbt test` as tasks (via `BashOperator`/`KubernetesPodOperator` running the dbt CLI, or `dbt-core`'s Python API); dbt manages the DAG *within* the transformation layer. Don't duplicate model dependencies in Airflow — one `dbt run --select fct_daily_sales+` task can replace a dozen individually-wired Airflow tasks.

### 4. Version the compiled SQL, not just the source
`dbt compile` renders Jinja to plain SQL you can diff in code review — useful for catching an accidentally-full-table-scan hiding behind a macro before it hits the shared STS in production.

### 5. Isolate dbt's STS from ad-hoc BI load
If dbt and Superset (Day 37) share one Thrift Server, a heavy nightly `dbt run` can starve daytime dashboards and vice versa. Either run dbt against a separate STS instance/namespace, or place it in its own fair-scheduler pool (Day 29).

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

### Exercise 3: Compare strategies
```python
# Extend exercise 36: implement the SAME fact model three ways —
#   1) append (no watermark guard) and show it duplicates on a second run
#   2) insert_overwrite by partition and show a full day's data is safely replaced
#   3) a MERGE-based upsert (ENABLE_ICEBERG=1) keyed by unique_key
# Compare row counts after running each build script twice in a row.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **`dbt run` model timings** (`target/run_results.json`) — which model is slow, and is it getting slower over time?
2. **STS driver load during dbt runs** — CPU/memory/active-session count while `dbt run` executes (threads too high?).
3. **Test pass/fail counts** over time — a model whose tests intermittently fail points at an upstream data-quality issue, not a dbt bug.
4. **Freshness check results** (`dbt source freshness`) — is the bronze/raw layer arriving on schedule?

### Spark UI Analysis
- Each dbt model run shows up as one (or more) SQL statement in the STS's own Spark UI (Day 29) — use the **SQL tab** to find which compiled model is the heaviest query in a `dbt run`, exactly as you would for a raw analyst query.
- Compare the **Jobs** tab timeline across a `dbt run` to spot models running serially that could parallelize (dbt's `threads` setting), or models contending for the STS driver at the same moment.

## 🚨 Common Issues & Solutions

### Issue 1: Incremental model duplicates rows
**Symptom**: counts grow on re-run (e.g. an Airflow retry re-runs `dbt run`).
**Root Cause**: `incremental_strategy='append'` with a watermark filter that isn't idempotent (e.g. based on wall-clock "now" instead of the data itself), or no `unique_key` guard at all.
**Solution**: use `merge` with a `unique_key`, or `insert_overwrite` by partition — not bare `append`, unless the upstream source already guarantees exactly-once delivery.

### Issue 2: dbt overwhelms the Thrift Server
**Symptom**: STS slow/OOM during `dbt run`; other BI users see timeouts at the same time.
**Root Cause**: `threads` set too high relative to STS driver capacity, or several `table`-materialized models doing full rebuilds concurrently.
**Solution**: lower `threads`, use fair-scheduler pools (Day 29) to give dbt its own lane, and convert big `table` models to `incremental` so nightly runs process less data.

### Issue 3: First incremental run behaves differently than expected
**Symptom**: a "new" incremental model either scans everything (expected but surprising) or fails because `{{ this }}` doesn't exist yet.
**Root Cause**: the `is_incremental()` macro only returns true when the target table **already exists**; a fresh model's first run is always a full build, but a full rebuild triggered by `--full-refresh` also bypasses the incremental filter — teams sometimes forget this and expect `--full-refresh` to be a no-op.
**Solution**: treat `--full-refresh` as an explicit, deliberate operation (e.g. after a schema change), and make sure any downstream MERGE keys still dedupe correctly on a full rebuild.

### Issue 4: Schema drift breaks `SELECT *`-style models
**Symptom**: a model referencing `{{ ref('stg_transactions') }}` starts failing or silently drops a new upstream column.
**Root Cause**: an upstream table gained a column (Day 38 schema evolution) but the incremental model's compiled `MERGE`/`INSERT` has a fixed column list from an earlier run.
**Solution**: for `merge`, ensure the model's `SELECT` explicitly lists columns (safer than `*`) and add the new column deliberately; for Iceberg-backed models, combine with Iceberg's schema evolution (Day 33) rather than relying on implicit behavior.

### Issue 5: Snapshots lose intermediate history
**Symptom**: a customer that changed twice between two `dbt snapshot` runs only shows the latest of the two changes in `customers_snapshot`.
**Root Cause**: `dbt snapshot` only captures the source's state *at the moment it runs* — it has no way to see a change that was overwritten in the source before the next scheduled snapshot.
**Solution**: schedule `dbt snapshot` frequently enough relative to the source's real change rate, or, if every intermediate change must be captured, source from a true CDC log (Day 38) instead of periodic snapshots of current state.

## 📝 Key Takeaways
1. dbt = SQL transforms + tests + docs + lineage; Spark executes via `dbt-spark`.
2. Connect through the Thrift Server (`method: thrift`); STS tuning from Day 29 applies directly.
3. Use `incremental` + `merge` (Iceberg) for large, frequently-updated facts; `insert_overwrite` for partition-shaped Hive/Parquet tables.
4. Sources + tests + freshness checks turn dbt into a lightweight governance/catalog layer, not just a SQL runner.
5. Run `dbt test` as a pre-publish quality gate — a failed test should fail the Airflow task.
6. Airflow schedules `dbt run`/`dbt test`; dbt owns model lineage — don't duplicate the dependency graph in both places.
7. Isolate dbt's STS load from ad-hoc BI load with separate instances or fair pools.
8. `dbt snapshot` gives SCD Type 2 history without hand-written MERGE logic — but only as often as it runs.

## 🔗 Next Steps
- **Day 37**: Superset Query Optimization

## 📚 Additional Resources
- dbt-spark adapter docs (connection methods, `file_format`, `incremental_strategy`)
- dbt incremental models and materializations reference
- dbt tests and `sources.yml` / freshness docs

---

**Progress**: Day 36/40 ✅
