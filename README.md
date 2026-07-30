# Spark Deep Dive — 40-Day Mastery Path 🚀

> **Mission**: Master Apache Spark for on-premise data platforms — design, optimize, and troubleshoot Spark jobs at production level.

Every lesson has a matching **runnable** exercise. You do **not** need the production
cluster: a one-command [local environment](environment/README.md) gives you a real
multi-executor Spark UI on your laptop.

## 🎯 Learning Objectives

By the end of this journey, you will be able to:
- **Design** efficient Spark jobs for ETL pipelines
- **Optimize** Spark applications for performance and resource utilization
- **Troubleshoot** and fix common Spark errors and performance issues
- **Tune** Spark configurations for on-premise Kubernetes clusters
- **Master** Spark Thrift Server, Structured Streaming, Iceberg, and PySpark best practices

## 🛠️ Target Tech Stack
- **Platform**: On-premise Kubernetes cluster (Spark on K8S), MinIO/S3 storage
- **Storage**: Hive, Iceberg
- **Processing**: Spark (Thrift Server, PySpark, Structured Streaming)
- **Orchestration**: Airflow · **Transformation**: DBT · **Visualization**: Superset

## 🚀 Getting Started (5 minutes)

```bash
# 1. Stand up the local minikube cluster (Spark Operator + MinIO + History Server)
#    setup.sh builds the Dockerfile image, which already pip installs
#    environment/requirements.txt for you -- nothing to install locally.
bash environment/setup.sh
# 2. Generate laptop-friendly sample data (~1M rows) -- see environment/README.md
# 3. Run Day 1's exercise and open http://localhost:4040
kubectl apply -f environment/k8s/05-example-sparkapplication.yaml
```

> `environment/requirements.txt` only matters if you want optional **local** tooling —
> inspecting Parquet output with pandas, running lessons as local Jupyter notebooks, or
> running the Kafka producer on your laptop instead of as a pod. It's not needed to
> follow the k8s path above; run `pip install -r environment/requirements.txt` yourself
> only if you want one of those.

Prefer a real cluster feel? Use the [minikube Kubernetes cluster](environment/README.md)
(Spark Operator + MinIO/S3 + History Server — real driver/executor pods). Then read
[Day 1: Execution Model](01-fundamentals/day-01-execution-model.md) and follow
[GETTING_STARTED.md](GETTING_STARTED.md) for the daily routine.

## 🗺️ The Path — 5 Phases, 40 Days

| Phase | Days | Focus | Directory | Status |
|-------|------|-------|-----------|--------|
| **1. Deep Fundamentals** | 1–7 | Execution model, Catalyst, memory, shuffle, partitioning, joins, caching | [`01-fundamentals/`](01-fundamentals/) | ✅ Lessons authored |
| **2. Performance Tuning** | 8–14 | Config, resources, skew, shuffle/join/memory/network tuning | [`02-performance-tuning/`](02-performance-tuning/) | ✅ Lessons authored |
| **3. Troubleshooting Mastery** | 15–21 | Errors, OOM, task/shuffle failures, serialization, incident response | [`03-troubleshooting/`](03-troubleshooting/) | ✅ Lessons authored |
| **4. Advanced Topics** | 22–28 | Catalyst rules, advanced SQL, UDF/AQE, broadcast, bucketing, DPP, CBO | [`04-advanced-topics/`](04-advanced-topics/) | ✅ Lessons authored |
| **5. Production & Ecosystem** | 29–40 | Thrift, Streaming, PySpark/Zeppelin, Iceberg, Airflow, DBT, Superset, patterns | [`05-`…`10-`](10-production-patterns/) | ✅ Lessons authored |

All 40 lessons and their matching exercises are written — "status" above tracks lesson content, not
*your* personal study progress. Track that day-by-day in **[PROGRESS.md](PROGRESS.md)**.

## 📁 Repository Structure

```
spark-learning/
├── environment/              # 🧪 Local Spark cluster + sample-data generator (start here)
├── common/                   # Shared SparkSession factory used by every exercise
├── 01-fundamentals/          # Phase 1 — Days 1-7
├── 02-performance-tuning/    # Phase 2 — Days 8-14
├── 03-troubleshooting/       # Phase 3 — Days 15-21
├── 04-advanced-topics/       # Phase 4 — Days 22-28
├── 05-real-world-scenarios/  # Phase 5 — ETL/CDC (Day 38)
├── 06-spark-thrift/          # Phase 5 — Thrift Server (Day 29)
├── 07-structured-streaming/  # Phase 5 — Streaming (Days 30-31)
├── 08-pyspark-zeppelin/      # Phase 5 — PySpark/Zeppelin (Day 32)
├── 09-iceberg-integration/   # Phase 5 — Iceberg (Days 33-34)
├── 10-production-patterns/   # Phase 5 — Orchestration & patterns (Days 35-40)
├── exercises/                # Hands-on, runnable exercises (+ solutions/)
├── assessments/              # Per-phase self-assessments, mastery checklist, capstones
├── interview-prep/           # Interview question banks + incident drills
├── code-samples/             # Reference implementations
└── notes/                    # Your learning notes
```

## 📚 Supporting Docs
- [GETTING_STARTED.md](GETTING_STARTED.md) — daily routine & learning tips
- [QUICK_REFERENCE.md](QUICK_REFERENCE.md) — config cheat sheet
- [environment/README.md](environment/README.md) — local cluster & data setup
- [assessments/mastery-checklist.md](assessments/mastery-checklist.md) — the competency checklist
- [TEMPLATE_day-lesson.md](TEMPLATE_day-lesson.md) — lesson format

## 📖 Resources
- Official Spark Documentation · Performance tuning guides · Community best practices · Real-world case studies

---

**Let's begin the journey! 🎓**
