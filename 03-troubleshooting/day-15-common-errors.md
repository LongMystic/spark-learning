# Day 15: Common Error Patterns & Reading Logs/Stack Traces

## 🎯 Learning Objectives
- Recognize the handful of error families that cause ~90% of Spark failures
- Read a Spark stack trace top-down and find the *real* cause (not the noise)
- Navigate driver-pod logs, executor-pod logs, and your log-aggregation stack on-premise Kubernetes
- Build a fast triage routine you can run on any failed job

## 📚 Core Concepts

### 1. The error families you will actually meet

| Family | Signature exception | Usually caused by |
|--------|--------------------|-------------------|
| Memory | `OutOfMemoryError`, `ExecutorLostFailure` + pod `OOMKilled` (exit 137) | skew, huge shuffle, `collect()`, wide rows |
| Shuffle | `FetchFailedException`, `MetadataFetchFailedException` | lost executor, disk full, network, GC pauses |
| Serialization | `NotSerializableException`, `Task not serializable` | capturing non-serializable objects in a closure/UDF |
| Data | `AnalysisException`, `SparkArithmeticException`, `NumberFormatException` | schema mismatch, bad casts, nulls, divide-by-zero |
| Resource | driver pod failed, executor pods stuck `Pending` forever | quota exhausted, wrong pod sizing, `LimitRange` caps |

**Key Points:**
- The *first* exception in the driver log is often a **symptom**; the *root cause* is usually an earlier failure on an executor.
- Spark retries tasks/stages — a job that "failed" may have failed the same task 4 times. Find task attempt #1's cause.

### 2. Anatomy of a Spark stack trace

```
org.apache.spark.SparkException: Job aborted due to stage failure:
  Task 12 in stage 8.0 failed 4 times, most recent failure:
  Lost task 12.3 in stage 8.0 (TID 943) (worker-7 executor 5):
  org.apache.spark.shuffle.FetchFailedException: Failed to connect to worker-3:7337
    at org.apache.spark.storage.ShuffleBlockFetcherIterator...
  Caused by: java.io.IOException: Connection reset by peer      <-- the real cause
```

Read it as: **what failed** (Task 12, stage 8) → **where** (executor 5 on worker-7) → **which exception** (`FetchFailedException`) → **`Caused by:`** (the actual root). Always scroll to the last `Caused by:`.

## 🔍 Deep Dive: A repeatable triage routine

### Step-by-Step Process
1. **Read the driver's final exception** — note the failing *stage* and *task*.
2. **Open Spark UI → Stages → the failed stage** — is one task an outlier (skew)? Big shuffle read? Spill?
3. **Open the failed task's executor log** (UI → Executors → stderr, or `kubectl logs`) — find `Caused by:`.
4. **Classify** into one of the families above.
5. **Correlate** with resources: GC time, container kills, disk usage on `spark.local.dir`.
6. **Form one hypothesis, change one thing, re-run.** Never change five configs at once.

### Getting the logs on-premise (Kubernetes)
Pods are **ephemeral** — once a pod is deleted its stdout/stderr is gone. So you read logs two ways: live via `kubectl` while the pod exists, and after the fact from your log-aggregation stack (Fluent Bit → Loki/Elasticsearch) plus the Spark History Server replaying event logs from `s3a://`.
```bash
# Driver pod log (cluster mode: the driver runs as a pod)
kubectl -n spark-jobs logs <driver-pod>

# All executor pods for the app (they carry a spark-role label)
kubectl -n spark-jobs logs -l spark-role=executor

# A crashed/restarted pod: read the PREVIOUS container's log before it's gone
kubectl -n spark-jobs logs <pod> --previous

# Why a pod died (OOMKilled, scheduling, image pull): the events + status
kubectl -n spark-jobs describe pod <pod>

# After the app ends, pods are gone -> query the aggregation stack, e.g. Loki:
#   {namespace="spark-jobs", app="daily-etl"} | logfmt
# History server for the Spark UI after the app ends (reads s3a://spark-events):
#   http://spark-history:18080
```

## 💡 Key Insights for On-Premise

### 1. Log locations differ from cloud
- **Driver (client mode)**: your terminal / the launching process's stdout+stderr.
- **Driver (cluster mode)**: the driver pod — `kubectl logs <driver-pod>` while it lives, then the aggregation stack.
- **Executors**: each runs in its own pod; `kubectl logs -l spark-role=executor` live, and the node's log agent (Fluent Bit/Filebeat) scrapes stdout/stderr into Loki/EFK for after-the-fact search.

### 2. Pods are ephemeral — ship logs before they vanish
`kubectl logs` only works while the pod exists; a completed/deleted executor pod takes its logs with it. That's why you (a) use `--previous` to catch a just-crashed container, and (b) rely on centralized aggregation + `spark.eventLog.dir=s3a://spark-events` so the History Server can replay the run after every pod is gone.

## 🎯 Practical Exercises

### Exercise 1: Trigger and classify
```python
# See exercises/troubleshooting/exercise-15-error-triage.py
# It deliberately raises one error from each family; practice classifying from the trace.
```

### Exercise 2: Find the root cause
```python
# Given a multi-line trace with nested "Caused by:", identify the true root
# and which executor/task produced it.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Stage "Failed Tasks" count** — >0 means retries happened; investigate even if the job "succeeded."
2. **Executor "Failed"/"Lost" count** — lost executors are the usual driver of `FetchFailed`.

### Spark UI Analysis
- Stages tab → failed stage → "Errors" column shows the per-task exception summary.
- Executors tab → red/removed executors → click stderr for the container log.

## 🚨 Common Issues & Solutions

### Issue 1: "It says AnalysisException but my code looks fine"
**Symptom**: fails instantly, before any job runs.
**Solution**: this is a *plan-time* (Catalyst) error — column/table/type problem. `df.printSchema()` and check names/case; it never reached the cluster.

### Issue 2: The driver trace is generic ("Job aborted due to stage failure")
**Symptom**: no obvious cause on the driver.
**Solution**: the cause is on an executor — open the failed task's executor stderr and read the last `Caused by:`.

## 📝 Key Takeaways
1. Five families cover almost everything: memory, shuffle, serialization, data, resource.
2. Read traces bottom-up to the last `Caused by:`.
3. The driver shows the symptom; the executor log shows the cause.
4. One hypothesis, one change, one re-run.
5. On Kubernetes, `kubectl logs` (with `--previous`) + your aggregation stack are your primary tools; pods are ephemeral.

## 🔗 Next Steps
- **Day 16**: OOM Debugging (Driver vs Executor)
- Practice: pull `kubectl logs` (and `--previous`) for one real failed job at work and classify it.

## 📚 Additional Resources
- Spark Monitoring and Instrumentation docs
- Kubernetes logging architecture + your cluster's log-aggregation stack (Fluent Bit → Loki/EFK)

---

**Progress**: Day 15/40 ✅
