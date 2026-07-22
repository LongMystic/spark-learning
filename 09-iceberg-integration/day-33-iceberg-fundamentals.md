# Day 33: Iceberg Fundamentals & Read/Write

## 🎯 Learning Objectives
- Understand what Apache Iceberg adds over plain Hive/Parquet tables
- Configure a Spark + Iceberg catalog on-premise
- Read/write Iceberg tables and use hidden partitioning
- Grasp snapshots and the metadata that powers time travel

## 📚 Core Concepts

### 1. Why Iceberg
Hive tables track partitions as directories in a metastore — slow listings, no atomic commits, no schema/partition evolution, no snapshot isolation. **Iceberg** is a table format with a metadata layer (snapshots → manifests → data files) that adds:
- **ACID commits** (atomic, concurrent-safe writes).
- **Snapshot isolation & time travel**.
- **Hidden partitioning** (partition without polluting the schema or queries).
- **Schema & partition evolution** without rewriting data.
- **File-level stats** for fast pruning.

### 2. Configure the catalog (on-prem, Hadoop/HDFS)
```python
spark = (SparkSession.builder
  .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog")
  .config("spark.sql.catalog.local.type", "hadoop")
  .config("spark.sql.catalog.local.warehouse", "hdfs:///warehouse/iceberg")
  .getOrCreate())
# (Our local env: set ENABLE_ICEBERG=1 to get exactly this, on the local filesystem.)
```
Production commonly uses a **Hive catalog** (`type=hive`) so Iceberg tables show up in the shared metastore alongside existing tables.

### 3. Create, write, read
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
`PARTITIONED BY (days(txn_ts))` derives the partition from `txn_ts` **automatically**. Queries filter on `txn_ts` directly (no extra `dt=` column, no user mistakes), and Iceberg prunes partitions transparently — a big ergonomics + correctness win over Hive-style partition columns.

## 🔍 Deep Dive: Snapshots
Every write creates a **snapshot** — an immutable view of the table at that commit. Metadata records the file list per snapshot, enabling:
```sql
-- inspect
SELECT * FROM local.db.transactions.snapshots;
SELECT * FROM local.db.transactions.files;
-- time travel (Day 34)
SELECT * FROM local.db.transactions VERSION AS OF 1234567890;
```

## 💡 Key Insights for On-Premise
### 1. Iceberg fixes the small-file & listing pain
No directory listing to find partitions — Iceberg reads manifests. Combined with compaction (Day 34), it's a strong answer to HDFS small-file problems that plague Hive tables.

### 2. Concurrent writers are safe
Iceberg's optimistic-concurrency commits let multiple jobs write without corrupting the table (retries on conflict). This makes CDC/upsert pipelines (Day 34/38) reliable, unlike naive Parquet overwrites.

### 3. Match Iceberg runtime to Spark version
Use the correct `iceberg-spark-runtime-<spark>_<scala>` jar via `--packages`/`--jars`. Version mismatches are the usual "catalog not found"/method errors on-prem.

## 🎯 Practical Exercises

### Exercise 1: Create & query an Iceberg table locally
```python
# See exercises/production/exercise-33-iceberg-fundamentals.py  (run with ENABLE_ICEBERG=1)
# Create an Iceberg table with hidden partitioning; append; inspect .snapshots and .files.
```

### Exercise 2: Hidden partitioning pruning
```python
# Filter on txn_ts and confirm partition pruning without a partition column in the schema.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Number of snapshots (grows with every write — expire them, Day 34).
2. Data files per partition (small-file health).

### Spark UI Analysis
- Scan node shows Iceberg pruning (fewer files read) driven by manifest stats.

## 🚨 Common Issues & Solutions

### Issue 1: "Catalog 'local' not found"
**Symptom**: config/jar problem.
**Solution**: add the matching `iceberg-spark-runtime` jar and the catalog + extensions configs.

### Issue 2: Too many snapshots/metadata files
**Symptom**: metadata bloat, slow planning.
**Solution**: expire snapshots and rewrite manifests (Day 34).

## 📝 Key Takeaways
1. Iceberg adds ACID, snapshots, time travel, hidden partitioning, and evolution.
2. Configure a Spark catalog (hadoop or hive `type`).
3. Hidden partitioning derives partitions from a column — cleaner + safer.
4. Every write is a snapshot; metadata enables pruning and time travel.
5. Iceberg is a strong on-prem answer to small-file and concurrency pain.

## 🔗 Next Steps
- **Day 34**: Iceberg Maintenance (Compaction, Snapshots, Time Travel, MERGE)

## 📚 Additional Resources
- Apache Iceberg + Spark docs

---

**Progress**: Day 33/40 ✅
