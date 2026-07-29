# 🎉 Repository Status

Your Spark Deep Dive learning repository is a **40-day, self-contained** curriculum.
Every lesson has a runnable exercise, and you can run all of it on your laptop.

## 📁 What Exists

### Core docs
- ✅ **README.md** — overview & 40-day path
- ✅ **GETTING_STARTED.md** — daily routine & tips
- ✅ **PROGRESS.md** — day-by-day tracker (authoritative status)
- ✅ **QUICK_REFERENCE.md** — config cheat sheet
- ✅ **TEMPLATE_day-lesson.md** — lesson format

### Local environment (run everything cluster-free)
- ✅ **environment/** — minikube Kubernetes setup (`setup.sh` + `k8s/` manifests: Spark Operator,
  MinIO/S3, History Server, namespaces+quotas), `generate_data.py` sample-data generator,
  `requirements.txt`, Kafka producer for streaming
- ✅ **common/spark_session.py** — shared SparkSession factory used by every exercise

### Lesson content
- ✅ **Phase 1 — Days 1-7** (`01-fundamentals/`): lessons + exercises
- ✅ **Phase 2 — Days 8-14** (`02-performance-tuning/`): lessons + exercises
- 🚧 **Phases 3-5 — Days 15-40** (`03-` … `10-`): being authored; see PROGRESS.md

### Practice & evaluation
- ✅ **exercises/** — runnable exercises (`fundamentals/`, `performance-tuning/`, and more as phases land)
- 🚧 **exercises/solutions/** — worked solutions + expected observations
- 🚧 **assessments/** — per-phase self-assessments, mastery checklist, capstones
- 🚧 **interview-prep/** — question banks + incident drills

## 🚀 Start Here

```bash
pip install -r environment/requirements.txt
See environment/README.md for data generation
kubectl apply -f environment/k8s/05-example-sparkapplication.yaml
```

Then read `01-fundamentals/day-01-execution-model.md` and follow `GETTING_STARTED.md`.

## 💡 Tips for Success
1. **Be consistent** — 30-45 min/day beats marathon sessions.
2. **Run the code** — always inspect the Spark UI after a job.
3. **Take notes** in `notes/`, and apply optimizations to real jobs at work.
4. **Check yourself** with the phase assessments in `assessments/`.

---

**You're all set — begin with Day 1! 🚀**
