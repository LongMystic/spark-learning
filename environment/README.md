# Local Environment 🧪

Run **every** exercise on your laptop — no access to the production cluster required.
You get a real multi-executor Spark UI, so shuffle, skew, and data-locality lessons are
actually observable.

There are two ways to run. Pick one.

---

## Option A — Local PySpark (fastest to start)

Best for reading plans, small data, and most fundamentals/tuning exercises.

```bash
# 1. Install (a virtualenv is recommended)
pip install -r environment/requirements.txt

# 2. Generate sample data (~1M rows, laptop-friendly)
python environment/generate_data.py --scale small

# 3. Run any exercise (from the repo root)
python exercises/fundamentals/exercise-01-dag-analysis.py
```

Spark runs in-process as `local[*]`. The per-application UI is at **http://localhost:4040**
while a job is running. Finished runs are replayed by the History Server (Option B) or by
pointing a history server at the `spark-events/` folder.

> **Windows note:** local PySpark needs Java 8/11/17 and (sometimes) `winutils.exe`/`HADOOP_HOME`.
> If that's fiddly, use Option B (Docker) — it bundles everything.

---

## Option B — Docker standalone cluster (most realistic)

1 master + **2 workers** + a history server. This is the recommended way to *see* how work
spreads across executors.

```bash
# 1. Bring the cluster up
docker compose -f environment/docker-compose.yml up -d

# 2. Generate data INSIDE the cluster (writes to the shared data volume)
docker compose -f environment/docker-compose.yml exec spark-master \
  python /opt/spark-apps/environment/generate_data.py --scale small

# 3. Submit an exercise to the cluster
docker compose -f environment/docker-compose.yml exec spark-master \
  /opt/spark/bin/spark-submit --master spark://spark-master:7077 \
  /opt/spark-apps/exercises/fundamentals/exercise-01-dag-analysis.py
```

| UI | URL | Shows |
|----|-----|-------|
| Master | http://localhost:8080 | Workers, running/completed apps |
| Driver (live app) | http://localhost:4040 | Jobs, Stages, SQL, Executors |
| Worker 1 / 2 | http://localhost:8081 / 8082 | Per-worker executors & logs |
| History Server | http://localhost:18080 | Replay any finished app |

Tear down with `docker compose -f environment/docker-compose.yml down`
(add `-v` to also wipe the data + event-log volumes).

---

## Pointing exercises at data

Exercises read tables through [`common/spark_session.py`](../common/spark_session.py):

- `get_spark("name")` — SparkSession with sensible, observable defaults.
- `read_table(spark, "transactions")` — reads a generated parquet table.

Both honour environment variables so the **same code** runs locally or on prod:

| Variable | Default | Use |
|----------|---------|-----|
| `SPARK_MASTER` | `local[*]` | set to `spark://localhost:7077` or `yarn` |
| `DATA_DIR` | `<repo>/data` | point at your Hive/Iceberg export instead |
| `ENABLE_ICEBERG` | `0` | `1` adds a local Iceberg catalog (Days 33-34) |
| `SPARK_EVENTLOG` | `1` | write event logs for the History Server |

**Run against the real cluster instead of local data:**

```bash
export SPARK_MASTER=yarn
export DATA_DIR=hdfs:///warehouse/your_db      # or leave read_table and swap in your own reads
python exercises/performance-tuning/exercise-10-data-skew-handling.py
```

---

## Generated datasets

`generate_data.py` writes these Parquet tables into `DATA_DIR`:

| Table | Rows (small) | Notes |
|-------|--------------|-------|
| `customers` | 50K | dimension, medium cardinality |
| `products` | 500 | small → **broadcast-join** candidate |
| `stores` | 50 | tiny → broadcast candidate |
| `transactions` | ~1M | fact, **evenly** distributed, partitioned by `txn_date` |
| `transactions_skewed` | ~1M | fact, ~80% of rows on 5 hot customers → **skew** practice |

Scales: `--scale small` (~1M) · `medium` (~10M) · `large` (~50M, cluster recommended).

---

## Streaming (Days 30-31 only)

```bash
docker compose -f environment/docker-compose.yml --profile streaming up -d
python environment/produce_stream.py --rate 20 --topic transactions
```

Then run the Day 30-31 streaming exercises, which read from `localhost:9092`.
