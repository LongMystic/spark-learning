# Hands-On Exercises

Every exercise is **runnable** against the [local environment](../environment/README.md)
— no production cluster required. They import a shared SparkSession from
[`common/spark_session.py`](../common/spark_session.py) and read the generated sample
tables via `read_table(spark, "transactions")`, so the same code also runs on your
on-prem cluster (just set `SPARK_MASTER` / `DATA_DIR`).

## 📁 Structure

```
exercises/
├── fundamentals/          # Days 1-7   (Phase 1)   ✅
├── performance-tuning/    # Days 8-14  (Phase 2)   ✅
├── troubleshooting/       # Days 15-21 (Phase 3)
├── advanced/              # Days 22-28 (Phase 4)
├── streaming/             # Days 30-31 (Phase 5)
├── production/            # Days 29, 32-40 (Phase 5)
└── solutions/             # Worked solutions + expected observations
```

## 🚀 How to Use

```bash
# One-time setup
pip install -r ../environment/requirements.txt
python ../environment/generate_data.py --scale small

# Run an exercise (from the repo root so `common` is importable)
python exercises/fundamentals/exercise-01-dag-analysis.py
```

1. Read the day's lesson first.
2. Run the exercise and **open the Spark UI** (`http://localhost:4040` while it runs).
3. Answer the "Analysis Questions" at the bottom of each script.
4. Compare with `solutions/` only after attempting.
5. Experiment — change configs, scale the data up, and observe the difference.

## 💡 Tips
- Many timing comparisons only mean something at `--scale medium`/`large` or on a real
  cluster; on tiny local data, focus on the **plans** (`explain()`) and **UI metrics**,
  not wall-clock seconds.
- Settings like `spark.executor.instances` are submit-time (YARN) — locally they're printed
  for reference, not applied.

---

**Start with**: `fundamentals/exercise-01-dag-analysis.py`
