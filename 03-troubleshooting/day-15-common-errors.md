# Day 15: Common Error Patterns & Reading Logs/Stack Traces

## 🎯 Learning Objectives
- Recognize the handful of error families that cause ~90% of Spark failures
- Read a Spark stack trace top-down and find the *real* cause (not the noise)
- Navigate driver logs, executor logs, and YARN aggregated logs on-premise
- Build a fast triage routine you can run on any failed job

## 📚 Core Concepts

### 1. The error families you will actually meet

| Family | Signature exception | Usually caused by |
|--------|--------------------|-------------------|
| Memory | `OutOfMemoryError`, `ExecutorLostFailure (killed by YARN ... exceeds memory)` | skew, huge shuffle, `collect()`, wide rows |
| Shuffle | `FetchFailedException`, `MetadataFetchFailedException` | lost executor, disk full, network, GC pauses |
| Serialization | `NotSerializableException`, `Task not serializable` | capturing non-serializable objects in a closure/UDF |
| Data | `AnalysisException`, `SparkArithmeticException`, `NumberFormatException` | schema mismatch, bad casts, nulls, divide-by-zero |
| Resource | `ApplicationMaster ... FAILED`, container pending forever | queue full, wrong executor sizing, YARN limits |

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
3. **Open the failed task's executor log** (UI → Executors → stderr, or YARN logs) — find `Caused by:`.
4. **Classify** into one of the families above.
5. **Correlate** with resources: GC time, container kills, disk usage on `spark.local.dir`.
6. **Form one hypothesis, change one thing, re-run.** Never change five configs at once.

### Getting the logs on-premise (YARN)
```bash
# Aggregated logs for a finished application
yarn logs -applicationId application_1699999999999_1234 > app.log

# Just one container / executor
yarn logs -applicationId application_..._1234 -containerId container_..._000005

# The RM UI: http://resource-manager:8088  -> your app -> logs
# History server for the Spark UI after the app ends: http://history-server:18080
```

## 💡 Key Insights for On-Premise

### 1. Log locations differ from cloud
- **Driver (client mode)**: your terminal / the launching process's stdout+stderr.
- **Driver (cluster mode)**: inside the AM container — use `yarn logs`.
- **Executors**: on each NodeManager under `yarn.nodemanager.log-dirs`, aggregated to HDFS after the app ends.

### 2. Log aggregation timing
Logs may only appear in `yarn logs` **after** the app finishes. For a hung job, read the live container logs from the NodeManager UI instead of waiting.

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
5. On YARN, `yarn logs -applicationId` is your primary tool.

## 🔗 Next Steps
- **Day 16**: OOM Debugging (Driver vs Executor)
- Practice: pull `yarn logs` for one real failed job at work and classify it.

## 📚 Additional Resources
- Spark Monitoring and Instrumentation docs
- YARN log aggregation documentation

---

**Progress**: Day 15/40 ✅
