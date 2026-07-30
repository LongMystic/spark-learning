# Hands-On Exercises

Every exercise is **runnable** against the [Kubernetes environment](../environment/README.md).
They import a shared SparkSession from [`common/spark_session.py`](../common/spark_session.py)
and read the generated sample tables via `read_table(spark, "transactions")`.

## 📁 Structure

```
exercises/
├── fundamentals/          # exercise-00 (bonus intro) + Days 1-7   (Phase 1)
├── performance-tuning/    # Days 8-14  (Phase 2)
├── troubleshooting/       # Days 15-21 (Phase 3)
├── advanced/              # Days 22-28 (Phase 4)
├── streaming/             # Days 30-31 (Phase 5)
├── production/            # Days 29, 32-40 (Phase 5)
└── solutions/             # Worked solutions + expected observations
```

`fundamentals/exercise-00-simple-spark-app.py` is a bonus, ungraded "hello world" DataFrame
script with no day mapping — run it first if you want to sanity-check your environment before
starting Day 1's `exercise-01-dag-analysis.py`.

## 🚀 How to Use

```bash
# Run an exercise via the Spark Operator CRD:
kubectl apply -f environment/k8s/05-example-sparkapplication.yaml
```

1. Read the day's lesson first.
2. Submit the exercise and **open the live driver UI** (via port-forwarding 4040).
3. Answer the "Analysis Questions" at the bottom of each script.
4. Compare with `solutions/` only after attempting.
5. Experiment — change configs, scale the data up, and observe the difference.

## 💡 Tips
- Many timing comparisons only mean something at `--scale medium`/`large` or on a real
  cluster; on tiny local data, focus on the **plans** (`explain()`) and **UI metrics**,
  not wall-clock seconds.
- Settings like `spark.executor.instances` are submit-time on Kubernetes (`spark-submit` /
  the `SparkApplication` CRD) — locally they're printed for reference, not applied.

---

**Start with**: `fundamentals/exercise-01-dag-analysis.py`
