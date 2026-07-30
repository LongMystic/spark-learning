# Day 33: Iceberg Fundamentals & Read/Write

## 🎯 Learning Objectives
- Understand what Apache Iceberg adds over plain Hive/Parquet tables
- Configure a Spark + Iceberg catalog on-premise (filesystem/hadoop, hive, and REST catalog types)
- Read/write Iceberg tables and use hidden partitioning
- Grasp snapshots and the metadata layer that powers time travel and pruning
- Know how to inspect a table's metadata tables (`.snapshots`, `.files`, `.history`)

## 📚 Core Concepts

### 1. Why Iceberg

Hive tables track partitions as directories in a metastore — slow
listings, no atomic commits, no schema/partition evolution, no snapshot
isolation. **Iceberg** is a table format with a metadata layer (snapshots →
manifests → data files) that adds:

**Key Points:**
- **ACID commits** (atomic, concurrent-safe writes) — a write either fully succeeds and becomes a new snapshot, or fails without corrupting the table.
- **Snapshot isolation & time travel** — readers always see a consistent view as of some snapshot, and you can query any prior snapshot explicitly.
- **Hidden partitioning** (partition without polluting the schema or queries) — no more `WHERE dt = '2026-07-30'` gymnastics or forgetting the partition filter.
- **Schema & partition evolution** without rewriting data — add/rename/drop columns, or change partitioning going forward, without a full table rewrite.
- **File-level stats** for fast pruning — manifests store min/max/null-count per column per file, so Spark can skip whole files without listing directories.

**Example:**
```sql
-- Hive-style: a "dt" column you must remember to filter on, directory listing per query
-- vs Iceberg: filter on the real timestamp column, metadata (not listing) drives pruning
```

### 2. Configure the catalog (on-prem, MinIO/S3)

**Key Points:**
- `spark.sql.extensions` registers Iceberg's SQL extensions (adds `MERGE INTO` semantics for Iceberg tables, `CALL` procedures, `ALTER TABLE ... ADD PARTITION FIELD`, etc.).
- `spark.sql.catalog.<name>` registers a named catalog; `local` here is just a catalog name you choose, not a Spark keyword.
- `type=hadoop` is Iceberg's **filesystem catalog** implementation — it tracks tables via files under the warehouse path on whatever `FileSystem` implementation the path scheme resolves to (`s3a://` for MinIO/S3, local `file://` for a laptop). It is *not* the YARN/HDFS stack; "hadoop" just names the Iceberg catalog impl.
- Production commonly uses a **Hive catalog** (`type=hive`, pointing at a Hive Metastore `thrift://` URI) so Iceberg tables show up in a shared metastore alongside existing Hive tables and are visible to Spark Thrift Server (Day 29) and DBT (Day 37) the same way. A **REST catalog** (`type=rest`) is the newer, engine-agnostic option when multiple engines beyond Spark need a common catalog service.

**Example:**
```python
spark = (SparkSession.builder
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.local.type", "hadoop")
  .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/iceberg")
  .getOrCreate())
# (Our local env: set ENABLE_ICEBERG=1 to get exactly this, on the local filesystem.)
```
```python
# Production-shaped alternative: Hive catalog, shared with the metastore
# .config("spark.sql.catalog.local.type", "hive")
# .config("spark.sql.catalog.local.uri", "thrift://hive-metastore:9083")
```

### 3. Create, write, read

**Key Points:**
- `USING iceberg` marks the table as an Iceberg table at creation time; without it, `CREATE TABLE` under an Iceberg-enabled catalog would still default to Iceberg in many setups, but being explicit avoids surprises.
- `writeTo(...).append()` is Iceberg's DataFrame write path (the "v2" `DataFrameWriterV2` API) — it also supports `.overwritePartitions()`, `.replace()`, and `.createOrReplace()` for other write semantics beyond plain append.
- Reads are ordinary `spark.read.table(...)` or `spark.sql(...)` — from the query side, an Iceberg table looks like any other table; the format-specific behavior (pruning, time travel, snapshots) is additive, not a different query syntax.

**Example:**
```sql
CREATE TABLE local.db.transactions (
    txn_id BIGINT, customer_id BIGINT, amount DOUBLE, txn_ts TIMESTAMP)
USING iceberg
PARTITIONED BY (days(txn_ts));          -- hidden partitioning by day
```
```python
df.writeTo("local.db.transactions").append()
spark.read.table("local.db.transactions").where("txn_ts >= '2026-07-01'").show()
```

### 4. Hidden partitioning

**Key Points:**
- `PARTITIONED BY (days(txn_ts))` derives the partition from `txn_ts` **automatically**, using an Iceberg **partition transform** (`days`, `hours`, `months`, `years`, `bucket(N, col)`, `truncate(N, col)` are the common ones).
- Queries filter on `txn_ts` directly (no extra `dt=` column, no user mistakes), and Iceberg prunes partitions transparently — a big ergonomics + correctness win over Hive-style partition columns, where forgetting the `dt=` filter silently forces a full scan.
- Partition transforms can be **changed going forward** via `ALTER TABLE ... ADD PARTITION FIELD` without rewriting historical data — old data keeps its old partitioning, new data uses the new scheme, and Iceberg's planner understands both.
- `bucket(N, col)` is the transform to reach for on a high-cardinality join/filter key (e.g. `customer_id`) when you want partition-pruning-like benefits without a natural time/category column to partition by.

**Example:**
```sql
-- Hidden partitioning by day (most common for time-series fact tables)
CREATE TABLE local.db.transactions (...)
USING iceberg PARTITIONED BY (days(txn_ts));

-- Bucket transform for a high-cardinality key with no natural time dimension
CREATE TABLE local.db.customers (...)
USING iceberg PARTITIONED BY (bucket(16, customer_id));

-- Evolve partitioning later, without rewriting existing data
ALTER TABLE local.db.transactions ADD PARTITION FIELD hours(txn_ts);
```

### 5. Schema evolution

**Key Points:**
- Iceberg tracks columns by a stable **column ID**, not by position — this is what makes add/rename/drop/reorder operations safe without rewriting existing data files; old files are read using the schema they were written with, reconciled against the current schema by ID.
- Supported evolutions include adding a column (existing rows read back as `NULL` for it), renaming a column (no data rewrite — the ID is unchanged), widening a type (e.g. `int` to `long`), dropping a column, and reordering columns.
- Evolution is done via ordinary `ALTER TABLE` DDL — there's no special Iceberg-only syntax to learn beyond the partition-evolution `ADD/DROP PARTITION FIELD` statements already shown.
- This is safe specifically because every read goes through the catalog and the table's current metadata — bypassing the catalog (e.g. reading raw Parquet files directly) loses this safety net (Common Issues #4).

**Example:**
```sql
ALTER TABLE local.db.transactions ADD COLUMN channel STRING;
ALTER TABLE local.db.transactions RENAME COLUMN amount TO amount_usd;
ALTER TABLE local.db.transactions ALTER COLUMN customer_id TYPE BIGINT;
```

## 🔍 Deep Dive: Snapshots and the metadata layer

### Step-by-Step Process

1. **A write happens** (`append`, `overwrite`, `MERGE`, etc.) and Iceberg writes new **data files** (Parquet/ORC/Avro) to the warehouse location.
2. **Iceberg writes one or more manifest files** listing exactly which data files belong to the new state, along with per-file column statistics (min/max/null counts) used for pruning.
3. **A manifest list** ties a set of manifests together into a single **snapshot** — an immutable, atomically-committed view of "the table as of this write."
4. **The table's metadata file (`metadata.json`) is atomically swapped** to point at the new snapshot — this atomic pointer swap is what gives Iceberg ACID commits; readers already in flight keep reading the old (still valid) snapshot.
5. **Old snapshots remain queryable** until explicitly expired (Day 34's `expire_snapshots`) — this is what powers time travel, and also why storage isn't immediately reclaimed after a write that "removes" data.

### Example: Inspecting metadata tables

```sql
-- inspect
SELECT * FROM local.db.transactions.snapshots;
SELECT * FROM local.db.transactions.files;
SELECT * FROM local.db.transactions.history;
-- time travel (Day 34 covers this in depth)
SELECT * FROM local.db.transactions VERSION AS OF 1234567890;
```

**Analysis:**
- `.snapshots` shows every committed snapshot with its `snapshot_id`, `committed_at`, `operation` (append/overwrite/replace/delete), and the parent snapshot — effectively the table's commit log.
- `.files` shows the current set of live data files with size and record-count stats — a fast way to check for the small-file problem (Day 34) without touching the object store directly.
- `.history` shows the sequence of snapshots over time, including which ones were the result of a rollback — useful for auditing "what happened to this table and when."
- None of these metadata tables require listing the underlying object store directory — they're read straight from the manifest/manifest-list files, which is the core reason Iceberg avoids Hive's slow-listing problem on S3/MinIO-style storage.

## 💡 Key Insights for On-Premise

### 1. Iceberg fixes the small-file & listing pain
No directory listing to find partitions — Iceberg reads manifests.
Combined with compaction (Day 34), it's a strong answer to object-store
small-file problems that plague Hive tables (listing many tiny objects on
S3/MinIO is slow and expensive, and MinIO on-prem has no CDN/edge caching to
hide that latency the way a managed cloud object store sometimes does).

### 2. Concurrent writers are safe
Iceberg's optimistic-concurrency commits let multiple jobs write without
corrupting the table (retries on conflict — a writer whose snapshot base
changed underneath it retries against the new base rather than silently
overwriting). This makes CDC/upsert pipelines (Day 34/38) reliable, unlike
naive Parquet overwrites where two concurrent writers can clobber each
other's output.

### 3. Match Iceberg runtime to Spark version
Use the correct `iceberg-spark-runtime-<spark>_<scala>` jar via
`--packages`/`--jars` (e.g. `iceberg-spark-runtime-3.5_2.12`). Version
mismatches are the usual "catalog not found"/method-not-found errors
on-prem — bake the matching jar into the Spark image (Day 17) rather than
relying on `--packages` pulling it at job-submit time in an
internet-restricted cluster.

### 4. Catalog choice affects the rest of the platform
Choosing `hadoop`/filesystem catalog is the fastest way to get started
locally, but a `hive` or `rest` catalog is what lets STS (Day 29), DBT (Day
37), and any other engine on the cluster see the **same** tables through
the **same** metastore — plan the catalog choice around what else needs to
query these tables, not just this one Spark job.

### 5. Metadata tables are your first debugging stop
Before reaching for `kubectl`/MinIO console to investigate "why is this
table slow" or "how much data is here," query `.snapshots`, `.files`, and
`.history` directly — they answer most day-to-day operational questions
(file counts, snapshot growth, recent write operations) without leaving
Spark SQL, and they're exactly what Day 34's maintenance procedures act on.

## 🎯 Practical Exercises

### Exercise 1: Create & query an Iceberg table locally
```python
# See exercises/production/exercise-33-iceberg-fundamentals.py  (run with ENABLE_ICEBERG=1)
spark.sql("CREATE NAMESPACE IF NOT EXISTS local.db")
spark.sql("""
    CREATE TABLE local.db.txn_ice (
        txn_id BIGINT, customer_id BIGINT, amount DOUBLE, txn_ts TIMESTAMP)
    USING iceberg
    PARTITIONED BY (days(txn_ts))
""")

(txns.select("txn_id", "customer_id", "amount", "txn_ts")
     .writeTo("local.db.txn_ice").append())
print("appended rows:", spark.table("local.db.txn_ice").count())

# Hidden partitioning: filter on txn_ts (no partition column in the schema!)
spark.table("local.db.txn_ice").where("txn_ts >= current_date() - interval 3 days").explain()

# Snapshots and files metadata
spark.sql("SELECT snapshot_id, committed_at, operation FROM local.db.txn_ice.snapshots").show(truncate=False)
print("files:", spark.sql("SELECT * FROM local.db.txn_ice.files").count())
```

### Exercise 2: Hidden partitioning pruning
```python
# Filter on txn_ts and confirm partition pruning without a partition column
# in the schema. Compare explain() output with and without the filter, and
# check the "PartitionFilters"/pruned-file-count in the plan.
spark.table("local.db.txn_ice").where("txn_ts >= '2026-07-28'").explain(True)
```

### Exercise 3: Bucket transform on a high-cardinality key
```python
# Create a second table partitioned with bucket(N, customer_id) instead of a
# time transform, and compare planning behavior for a customer_id filter.
spark.sql("""
    CREATE TABLE local.db.txn_by_customer (
        txn_id BIGINT, customer_id BIGINT, amount DOUBLE, txn_ts TIMESTAMP)
    USING iceberg
    PARTITIONED BY (bucket(16, customer_id))
""")
```

### Exercise 4: Schema evolution without a rewrite
```python
# Add a column, confirm old rows read back NULL for it, then check the
# snapshot/history tables show a metadata-only change (no new data files).
spark.sql("ALTER TABLE local.db.txn_ice ADD COLUMN channel STRING")
spark.table("local.db.txn_ice").select("txn_id", "channel").show(5)   # channel is NULL for existing rows

files_before = spark.sql("SELECT * FROM local.db.txn_ice.files").count()
# files count is unchanged -- schema evolution alone does not rewrite data files
print("files after ADD COLUMN:", files_before)
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Number of snapshots** — grows with every write; left unchecked, metadata bloats and planning slows (expire them, Day 34).
2. **Data files per partition** — a fast small-file health check via `SELECT * FROM table.files`.
3. **Manifest count and size** — many small manifests (from many small commits) slow planning the same way many small data files slow scans.
4. **Query planning time** — should stay flat as data grows if manifests/stats are healthy; a creeping planning time on an otherwise-unchanged query is a metadata-bloat signal.

### Spark UI Analysis
- The **SQL tab**'s scan node shows Iceberg pruning (fewer files read than exist in the table) driven by manifest stats — compare "files read" against the total in `.files` to confirm pruning is actually happening for your filter.
- The scan node's details also show which **partition filters** were pushed down — useful for confirming a hidden-partitioning filter (e.g. on `txn_ts`) was recognized and converted into a partition-level prune, not just a file-level filter.
- Planning time for an Iceberg scan (visible as the gap before the job's first task starts) grows with manifest count — a sudden increase across otherwise-similar queries is worth cross-checking against snapshot/manifest counts.

## 🚨 Common Issues & Solutions

### Issue 1: "Catalog 'local' not found"
**Symptom**: `AnalysisException` — the named catalog can't be resolved, or `CREATE TABLE ... USING iceberg` fails.
**Root Cause**: Missing `iceberg-spark-runtime` jar on the classpath, missing `spark.sql.extensions`, or a typo/mismatch in `spark.sql.catalog.<name>` config keys.
**Solution**: Add the matching `iceberg-spark-runtime-<spark>_<scala>` jar (via `--packages` or baked into the image) and the full catalog + extensions configs shown in Core Concepts #2 — confirm the catalog name in your SQL (`local.db.table`) matches the `spark.sql.catalog.local.*` prefix used in config.

### Issue 2: Too many snapshots/metadata files
**Symptom**: Metadata bloat, slow query planning that gets worse over time even though data volume is stable.
**Root Cause**: Frequent small writes (e.g. a streaming `foreachBatch` MERGE every 10 seconds, Day 31) each create a new snapshot and manifest, and nothing is expiring old ones.
**Solution**: Expire snapshots and rewrite manifests on a schedule (Day 34's `expire_snapshots` and `rewrite_manifests` procedures).

### Issue 3: Query doesn't prune partitions despite a filter on the partition column
**Symptom**: `explain()` shows most/all files being scanned even though the query filters on `txn_ts`.
**Root Cause**: The filter is on a transformed/derived expression Iceberg can't map back to the partition transform (e.g. `date_trunc` combinations that don't align with `days(txn_ts)`), or the filter is wrapped in a UDF the optimizer can't push down.
**Solution**: Filter directly on the partitioned column using comparable literal/timestamp expressions (as in the examples above), and check the physical plan's pushed filters to confirm the predicate Iceberg actually saw.

### Issue 4: Schema evolution breaks a downstream reader
**Symptom**: A consumer (STS, DBT model) errors or silently gets `NULL`s after a column was added/renamed upstream.
**Root Cause**: Iceberg supports safe schema evolution (add/rename/drop/reorder columns without rewriting data) via column IDs rather than positions, but a reader hardcoding positional column access (e.g. some RDD-based code, or a very old Parquet reader bypassing Iceberg's metadata) can still be surprised.
**Solution**: Always read Iceberg tables through the catalog (`spark.read.table(...)`/SQL), never by pointing a raw Parquet reader at the underlying files, so schema evolution is interpreted correctly via Iceberg's column-ID mapping.

## 📝 Key Takeaways
1. Iceberg adds ACID commits, snapshot isolation, time travel, hidden partitioning, and safe schema/partition evolution over plain Hive/Parquet tables.
2. Configure a Spark catalog (`hadoop` filesystem catalog for local/simple setups, `hive`/`rest` for shared production metastores) plus the Iceberg SQL extensions.
3. Hidden partitioning derives partitions from a column via transforms (`days`, `bucket`, etc.) — cleaner and safer than Hive-style partition columns.
4. Every write is a snapshot, built from manifests and manifest lists; metadata (not directory listing) drives pruning and time travel.
5. Iceberg is a strong on-prem answer to small-file and concurrent-writer pain on object storage like MinIO.
6. Always query Iceberg tables through the catalog so schema evolution and pruning are interpreted correctly.
7. Schema evolution (add/rename/drop/reorder/widen) is metadata-only and safe because Iceberg tracks columns by ID, not position.

## 🔗 Next Steps
- **Day 34**: Iceberg Maintenance (Compaction, Snapshots, Time Travel, MERGE)

## 📚 Additional Resources
- Apache Iceberg + Spark integration documentation (DDL, catalogs, partition transforms)
- Iceberg table spec: snapshots, manifests, manifest lists
- `iceberg-spark-runtime` release/compatibility matrix for matching Spark/Scala versions

---

**Progress**: Day 33/40 ✅
