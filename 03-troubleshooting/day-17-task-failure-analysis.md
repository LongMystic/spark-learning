# Day 17: Task Failure & Retry Analysis

## 🎯 Learning Objectives
- Understand Spark's task → stage → job retry model and its config knobs
- Tell transient failures (retry helps) from deterministic ones (retry just wastes time)
- Diagnose stragglers and decide when speculation helps vs hurts
- Read the "N failed / M succeeded" task picture in the Spark UI

## 📚 Core Concepts

### 1. The retry hierarchy
```
Task attempt  --(fails)-->  retried up to spark.task.maxFailures (default 4)
Stage         --(FetchFailed)-->  retried up to spark.stage.maxConsecutiveAttempts (default 4)
Job           --(stage exhausts retries)-->  fails the whole job
```
- **`spark.task.maxFailures = 4`**: a single task can fail 3 times and still let the job pass on the 4th.
- A stage failure caused by `FetchFailedException` re-runs the *parent* (map) stage to regenerate lost shuffle files — expensive.

### 2. Transient vs deterministic

| Transient (retry helps) | Deterministic (retry is futile) |
|-------------------------|----------------------------------|
| Executor lost / node hiccup | `NullPointerException` in a UDF on a specific row |
| Network blip, `FetchFailed` | Bad cast / parse on specific data |
| Preempted / evicted pod | Divide-by-zero, schema mismatch |
| Transient disk-full | Non-serializable closure |

If the **same task index** fails all 4 attempts with the **same exception**, it's deterministic — a data or code bug, not infrastructure.

### 3. Speculation (stragglers)
`spark.speculation=true` relaunches slow task copies and takes whichever finishes first.
- **Helps**: a slow/failing node makes a few tasks lag.
- **Hurts**: skew (the slow task is slow because it has 10× the data — a duplicate is equally slow, wasting resources) and non-idempotent writes (duplicate output).

## 🔍 Deep Dive: Diagnosing a failing task

### Step-by-Step Process
1. UI → Stages → failed stage → sort tasks by **Status** and **Duration**.
2. Is it **the same task index** failing repeatedly? → deterministic; open its input partition.
3. Different tasks failing on the **same executor/host**? → bad node; blacklist/decommission.
4. `FetchFailed` on a stage? → a map-side executor died; the stage re-runs. Fix the *executor loss* (Day 16/18), not the reduce stage.
5. Check the task's **input split** — which file/partition? Reproduce locally on just that slice.

### Reproducing a deterministic row failure
```python
# Narrow to the offending partition/file, then to the row
suspect = spark.read.parquet("data/transactions").where("txn_date = '2026-07-01'")
suspect.where("amount IS NULL OR quantity = 0").show(truncate=False)
```

## 💡 Key Insights for On-Premise

### 1. Blacklisting / exclusion
`spark.excludeOnFailure.enabled=true` (a.k.a. blacklisting) stops scheduling on a node that keeps failing tasks — invaluable when one worker node has a bad disk. Exclusion still works **per-node** on Kubernetes. On shared clusters, one flaky node can fail many jobs; exclusion contains the blast radius, and the platform team can also `cordon`/`taint` the bad node so the scheduler stops placing any pods there.

### 2. Don't mask bugs with retries
Raising `spark.task.maxFailures` to 16 to "get past" a failure usually just delays a deterministic error by 4×. Fix the row/code instead.

## 🎯 Practical Exercises

### Exercise 1: Deterministic vs transient
```python
# See exercises/troubleshooting/exercise-17-task-failures.py
# A UDF throws on a specific value; observe all 4 attempts fail identically.
```

### Exercise 2: Straggler simulation
```python
# Create a skewed partition; enable speculation; observe duplicate slow tasks (and why it doesn't help).
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. **Failed Tasks** and **task attempt numbers** (`.0`, `.1`, `.2`, `.3`).
2. **Per-host failure counts** (bad-node signal).
3. **Duration distribution** (min/median/max) → stragglers/skew.

### Spark UI Analysis
- Stage page "Aggregated Metrics by Executor" reveals a single bad executor.
- Task table "Errors" column groups the repeated exception.

## 🚨 Common Issues & Solutions

### Issue 1: Job fails after ~4 identical task errors
**Symptom**: "Task X in stage Y failed 4 times."
**Solution**: deterministic — inspect that task's data/UDF; retries won't help.

### Issue 2: Speculation makes things worse
**Symptom**: duplicate tasks, more load, no speedup.
**Solution**: the lag is skew, not a slow node — fix skew (salting/AQE), disable speculation for that job.

## 📝 Key Takeaways
1. `spark.task.maxFailures=4` — same task + same error 4× ⇒ deterministic bug.
2. `FetchFailed` re-runs the parent map stage; fix the executor loss upstream.
3. Speculation cures slow nodes, not skew or non-idempotent writes.
4. Exclusion/blacklisting contains one bad node.
5. Reproduce deterministic failures on the single offending partition.

## 🔗 Next Steps
- **Day 18**: Shuffle Error Resolution (FetchFailed)
- Practice: find a stage with failed tasks and determine transient vs deterministic.

## 📚 Additional Resources
- Spark configuration: task/stage retry, speculation, exclusion

---

**Progress**: Day 17/40 ✅
