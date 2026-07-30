# Day 15: Common Error Patterns & Reading Logs/Stack Traces

## 🎯 Learning Objectives
- Recognize the handful of error families that cause ~90% of Spark failures
- Read a Spark stack trace top-down and find the *real* cause (not the noise)
- Navigate driver-pod logs, executor-pod logs, and your log-aggregation stack on-premise Kubernetes
- Understand the difference between plan-time (Catalyst) failures and runtime (executor) failures
- Build a fast triage routine you can run on any failed job

## 📚 Core Concepts

### 1. The error families you will actually meet

Almost every Spark failure you will debug in production falls into one of five families. Learning to recognize the *shape* of each family — before you even read the full trace — is the single biggest speed-up in troubleshooting.

| Family | Signature exception | Usually caused by |
|--------|--------------------|-------------------|
| Memory | `OutOfMemoryError`, `ExecutorLostFailure` + pod `OOMKilled` (exit 137) | skew, huge shuffle, `collect()`, wide rows |
| Shuffle | `FetchFailedException`, `MetadataFetchFailedException` | lost executor, disk full, network, GC pauses |
| Serialization | `NotSerializableException`, `Task not serializable` | capturing non-serializable objects in a closure/UDF |
| Data | `AnalysisException`, `SparkArithmeticException`, `NumberFormatException` | schema mismatch, bad casts, nulls, divide-by-zero |
| Resource | driver pod failed, executor pods stuck `Pending` forever | quota exhausted, wrong pod sizing, `LimitRange` caps |

**Key Points:**
- The *first* exception in the driver log is often a **symptom**; the *root cause* is usually an earlier failure on an executor.
- Spark retries tasks/stages — a job that "failed" may have failed the same task 4 times. Find task attempt #1's cause, not attempt #4's.
- Each family has a dramatically different fix. Misclassifying wastes an entire debug cycle (e.g. bumping executor memory for what is actually a serialization bug).
- Most families have a **plan-time** variant (fails in milliseconds, before any executor runs) and a **runtime** variant (fails deep inside a stage). Knowing which one you're looking at tells you *where* to even start looking.

**Example:**
```python
from pyspark.sql.functions import col, udf

# DATA family, plan-time: AnalysisException, fails instantly
txns.select("doess_not_exist")  # typo -> AnalysisException before any job runs

# DATA family, runtime: fails only when a task actually evaluates the row
txns.selectExpr("amount / (quantity - quantity) AS boom").collect()

# SERIALIZATION family: closure captures something unpicklable
handle = lambda: None
@udf("int")
def bad_udf(x):
    return 1 if handle else 0   # NotSerializableException / pickling error
```

### 2. Anatomy of a Spark stack trace

A raw Spark exception can be 50-200 lines long, mixing Spark's own scheduling code with your code and the true root cause. Learn to skip the noise.

```
org.apache.spark.SparkException: Job aborted due to stage failure:
  Task 12 in stage 8.0 failed 4 times, most recent failure:
  Lost task 12.3 in stage 8.0 (TID 943) (worker-7 executor 5):
  org.apache.spark.shuffle.FetchFailedException: Failed to connect to worker-3:7337
    at org.apache.spark.storage.ShuffleBlockFetcherIterator...
  Caused by: java.io.IOException: Connection reset by peer      <-- the real cause
```

Read it as: **what failed** (Task 12, stage 8) → **where** (executor 5 on worker-7) → **which exception** (`FetchFailedException`) → **`Caused by:`** (the actual root). Always scroll to the *last* `Caused by:` — Java/Scala wrap exceptions, and each wrap adds a layer of "what Spark was doing" but the bottom-most one is "what actually broke."

**Key Points:**
- `Job aborted due to stage failure` is Spark's *outer* wrapper — it tells you a job failed, not why.
- `Task N in stage S.A failed K times` tells you the task index (`N`), the stage id and attempt (`S.A`), and how many attempts were made.
- `Lost task 12.3 ... (worker-7 executor 5)` tells you exactly which pod/executor produced the failure — that's where you go for the executor log.
- Every `Caused by:` is one level deeper into the real cause. Read to the bottom, not the top.
- If there is no `Caused by:` at all, the top-level exception message usually *is* the cause (common for `AnalysisException`).

### 3. Plan-time vs runtime errors

**Plan-time (Catalyst) errors** happen while Spark is building/analyzing/optimizing the logical plan — before a single task is scheduled on an executor. They are fast, deterministic, and never show up in the Spark UI's Jobs tab because no job was ever submitted.

```python
# AnalysisException: column, table, or type problem caught during analysis
df.select("doesnotexist")
df.groupBy("category").agg(sum("not_a_column"))
spark.sql("SELECT * FROM missing_table")
```

**Runtime errors** happen once tasks are actually executing on executors — they show up as failed tasks/stages in the UI, and their root cause lives in an executor's log, not the driver's.

```python
# Only fails when a task actually evaluates a row with quantity == 0
df.selectExpr("amount / quantity AS ratio").collect()
```

**Key Points:**
- If a failure happens in milliseconds and no stage ever appears in the Spark UI, it's plan-time — check schema, column names/case, and types with `df.printSchema()`.
- If a failure happens minutes into a run, with stages visible in the UI, it's runtime — go to the failed stage/task and read the executor log.
- `explain()` on the DataFrame *before* running it can surface plan-time issues without spending cluster time.

### 4. Where each error surfaces on Kubernetes

| Error family | Where it appears |
|---|---|
| Plan-time `AnalysisException` | Driver process output only; no pod, no stage, no UI job |
| Runtime data error, serialization | Driver's final "Job aborted" trace **and** the specific executor pod's log |
| Memory (executor) | Executor pod log + `kubectl describe pod` showing `OOMKilled` |
| Memory (driver) | Driver pod log; the driver pod itself may be evicted/OOMKilled |
| Shuffle | Reducer-side executor log shows `FetchFailedException`; the *map-side* executor that died is the real story |
| Resource | Pod stuck `Pending`; `kubectl describe pod` shows scheduling events, not an exception at all |

### 5. Reading exception chains across languages

PySpark jobs actually run two stacks glued together — a JVM (the real Spark engine) and a Python process (your driver code, and per-task Python workers for UDFs). A PySpark exception often shows *both*.

```
Traceback (most recent call last):
  File "job.py", line 42, in <module>
    txns.select("doess_not_exist").show()
pyspark.sql.utils.AnalysisException: [UNRESOLVED_COLUMN.WITH_SUGGESTION]
  A column, variable, or function parameter with name `doess_not_exist`
  cannot be resolved. Did you mean one of the following? [`does_not_exist`]
```
versus a runtime failure inside a UDF, where PySpark wraps the *Python* traceback inside the *Java* stack trace:
```
org.apache.spark.SparkException: Job aborted due to stage failure: ...
Caused by: org.apache.spark.api.python.PythonException:
  Traceback (most recent call last):
    File "worker.py", line 619, in main
    ...
    File "job.py", line 17, in enrich
  ZeroDivisionError: division by zero
```

**Key Points:**
- `pyspark.sql.utils.AnalysisException` (and friends like `ParseException`) are Python-side wrappers around the JVM's Catalyst exceptions — they carry the same "plan-time" meaning described above.
- A `PythonException` embedded inside a `SparkException` means the failure happened **inside a Python UDF running on an executor** — the Python traceback nested inside is exactly where your bug is; skip past the Java frames around it.
- Newer Spark versions attach structured error classes (like `[UNRESOLVED_COLUMN.WITH_SUGGESTION]` above) to `AnalysisException` — these are worth reading closely, since Spark increasingly suggests the likely fix (e.g. the correctly-spelled column name) directly in the message.

## 🔍 Deep Dive: A repeatable triage routine

### Step-by-Step Process
1. **Read the driver's final exception** — note the failing *stage* and *task* (e.g. "Task 12 in stage 8.0").
2. **Open Spark UI → Stages → the failed stage** — is one task an outlier (skew)? Big shuffle read? Spill?
3. **Open the failed task's executor log** (UI → Executors → stderr, or `kubectl logs`) — find the last `Caused by:`.
4. **Classify** into one of the five families above.
5. **Correlate** with resources: GC time, container kills, disk usage on `spark.local.dir`, node health.
6. **Form one hypothesis, change one thing, re-run.** Never change five configs at once — you'll never know which one fixed it (or introduced a new problem).

### Example: working a real trace end to end

```
org.apache.spark.SparkException: Job aborted due to stage failure:
  Task 45 in stage 12.0 failed 4 times, most recent failure:
  Lost task 45.3 in stage 12.0 (TID 2210) (10.42.3.17 executor 7):
  ExecutorLostFailure (executor 7 exited caused by one of the running tasks)
  Reason: Container killed by YARN for exceeding memory limits.
  ...
```
On Kubernetes the equivalent line reads `Reason: Executor 7 killed by driver`, or you correlate with `kubectl describe pod spark-job-abc-exec-7` showing `State: Terminated, Reason: OOMKilled, Exit Code: 137`. Either way:
1. Task 45, stage 12, executor 7 — that's *where*.
2. `ExecutorLostFailure` + a kill reason — that's the *family* (Memory).
3. `kubectl describe pod` confirms `OOMKilled` and shows the memory limit vs the pod's resource requests.
4. Hypothesis: stage 12 is a big `groupBy` with skew → check the Stages tab for one task with a huge shuffle-read compared to its peers.
5. Fix and re-run: e.g. raise `spark.sql.shuffle.partitions`, or enable AQE skew join (see Day 16/18) — one change at a time.

### Getting the logs on-premise (Kubernetes)
Pods are **ephemeral** — once a pod is deleted its stdout/stderr is gone. So you read logs two ways: live via `kubectl` while the pod exists, and after the fact from your log-aggregation stack (Fluent Bit → Loki/Elasticsearch) plus the Spark History Server replaying event logs from `s3a://`.
```bash
# Driver pod log (cluster mode: the driver runs as a pod)
kubectl -n spark-jobs logs <driver-pod>

# All executor pods for the app (they carry a spark-role label)
kubectl -n spark-jobs logs -l spark-role=executor

# Follow a live driver pod as it runs (great for a job that's currently failing)
kubectl -n spark-jobs logs -f <driver-pod>

# A crashed/restarted pod: read the PREVIOUS container's log before it's gone
kubectl -n spark-jobs logs <pod> --previous

# Why a pod died (OOMKilled, scheduling, image pull): the events + status
kubectl -n spark-jobs describe pod <pod>

# List all pods for a SparkApplication to see which executor id maps to which pod
kubectl -n spark-jobs get pods -l sparkoperator.k8s.io/app-name=daily-etl

# Check the SparkApplication CRD's own status (spark-operator specific)
kubectl -n spark-jobs get sparkapplication daily-etl -o yaml

# After the app ends, pods are gone -> query the aggregation stack, e.g. Loki:
#   {namespace="spark-jobs", app="daily-etl"} | logfmt
# History server for the Spark UI after the app ends (reads s3a://spark-events):
#   http://spark-history:18080
```

**Analysis:**
- `kubectl logs -l spark-role=executor` dumps *all* executor logs interleaved — useful for a quick scan, but for one specific failing task you want the single pod named in the trace (`worker-7 executor 5` maps to a pod like `daily-etl-abc123-exec-5`).
- `describe pod` is often more informative than the log itself for memory/scheduling problems, because the kill happens at the cgroup/kubelet level, outside the JVM's own logging.
- The spark-operator's `SparkApplication` CRD status field mirrors much of what you'd get from the driver log summary (application state, executor state) without needing to grep logs at all.

### Example: a resource-family failure that never produces an exception at all

Not every failure gives you a stack trace. A `RESOURCE` failure often just looks like nothing is happening.

```bash
$ kubectl -n spark-jobs get pods -l spark-role=executor
NAME                          READY   STATUS    RESTARTS   AGE
daily-etl-abc-exec-1          0/1     Pending   0          14m
daily-etl-abc-exec-2          0/1     Pending   0          14m

$ kubectl -n spark-jobs describe pod daily-etl-abc-exec-1
Events:
  Type     Reason            Age   From               Message
  ----     ------            ----  ----               -------
  Warning  FailedScheduling  14m   default-scheduler  0/6 nodes are available:
           6 Insufficient memory.
```

**Analysis:**
- There is no exception anywhere — the driver simply waits, and eventually times out or the job appears "stuck." The Spark UI's Jobs tab shows a job with 0 active/completed tasks because none of its executors ever became `Running`.
- The signal lives entirely in `kubectl describe pod`'s **Events** section, not in any Spark log — this is why the triage routine explicitly includes checking pod status, not just reading traces.
- Common root causes: a namespace `ResourceQuota` already exhausted by other jobs, executor pod requests sized larger than any single node's free capacity, or a `LimitRange` capping per-pod memory below what `--executor-memory` + overhead requires.

## 💡 Key Insights for On-Premise

### 1. Log locations differ from cloud
- **Driver (client mode)**: your terminal / the launching process's stdout+stderr.
- **Driver (cluster mode)**: the driver pod — `kubectl logs <driver-pod>` while it lives, then the aggregation stack.
- **Executors**: each runs in its own pod; `kubectl logs -l spark-role=executor` live, and the node's log agent (Fluent Bit/Filebeat) scrapes stdout/stderr into Loki/EFK for after-the-fact search.
- Unlike a managed cloud Spark service, there is no vendor "job details" web page that already aggregated everything for you — the History Server (backed by `spark.eventLog.dir=s3a://...`) *is* your after-the-fact UI, and your log stack (Loki/EFK) *is* your after-the-fact log search.

### 2. Pods are ephemeral — ship logs before they vanish
`kubectl logs` only works while the pod exists; a completed/deleted executor pod takes its logs with it. That's why you (a) use `--previous` to catch a just-crashed container, and (b) rely on centralized aggregation + `spark.eventLog.dir=s3a://spark-events` so the History Server can replay the run after every pod is gone.

### 3. Set up your triage tools before you need them
Bookmark the History Server URL, know your namespace's log-aggregation query syntax (LogQL for Loki, Lucene for Elasticsearch), and know the `spark-role` label convention your spark-operator setup uses (`spark-role=driver` / `spark-role=executor`). Finding these for the first time during an incident wastes precious minutes — see Day 21.

### 4. Correlate pod events with Spark's own retry model
A `kubectl describe pod` timeline (scheduled → pulling image → running → OOMKilled) lines up with the Spark UI's stage/task timeline. When they disagree (e.g. Spark UI shows the task still "running" but the pod is already gone), trust `kubectl` — it's closer to the ground truth of what actually happened to the container.

## 🎯 Practical Exercises

### Exercise 1: Trigger and classify each error family
```python
# See exercises/troubleshooting/exercise-15-error-triage.py
# It deliberately raises one error from each family (data/plan-time,
# data/runtime, serialization) via a wrapped try/except and prints the
# exception TYPE. Practice classifying from the type + trace alone, then
# uncomment traceback.print_exc() to read the full trace and find the
# last "Caused by:".
```

### Exercise 2: Find the root cause in a multi-line trace
```python
# Given a trace like:
#   SparkException: Job aborted due to stage failure: Task 12 in stage 8.0 ...
#     FetchFailedException: Failed to connect to worker-3:7337
#   Caused by: IOException: Connection reset by peer
#
# Answer, in writing, before looking anything up:
#   1. Which stage and task index failed?
#   2. Which executor/pod produced the failure?
#   3. What is the LAST "Caused by:" line?
#   4. Which of the 5 error families does this belong to?
#   5. Is this plan-time or runtime?
```

### Exercise 3: Read a real failed job's logs on your cluster
```bash
# Pick (or intentionally break) a job running via the spark-operator
# environment/k8s/05-example-sparkapplication.yaml, then:
kubectl -n spark-jobs get pods -l sparkoperator.k8s.io/app-name=<app>
kubectl -n spark-jobs logs <driver-pod> | tail -100
kubectl -n spark-jobs describe pod <failed-exec-pod>
# Classify the failure and identify where the root cause lived
# (driver log vs executor log vs pod events).
```

### Exercise 4: Diagnose a stuck (Pending) job
```bash
# Given a job whose driver log shows nothing new for 10+ minutes:
kubectl -n spark-jobs get pods -l sparkoperator.k8s.io/app-name=<app>
kubectl -n spark-jobs describe pod <pending-exec-pod>
# Read the Events section. Is it "Insufficient memory/cpu", an image pull
# error, or a node affinity/taint mismatch? Classify as a RESOURCE-family
# issue even though there is no stack trace at all.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Stage "Failed Tasks" count** — >0 means retries happened; investigate even if the job "succeeded."
2. **Executor "Failed"/"Lost" count** — lost executors are the usual driver of `FetchFailed`.
3. **Job duration vs historical baseline** — a job that "succeeded" but took 3x longer likely hid retries; check the History Server for stage retry counts.
4. **Pod restart counts** (`kubectl get pods` `RESTARTS` column) — a high number signals a recurring crash, not a one-off.

### Spark UI Analysis
- Stages tab → failed stage → "Errors" column shows the per-task exception summary, grouped by exception type and count — a fast way to see if all failures share one cause.
- Executors tab → red/removed executors → click stderr for the container log directly from the UI (works only while the executor pod still exists).
- Jobs tab → a job with 0 entries for a query you *know* you ran means it failed at plan time and never became a job at all — go back to the driver's raw output, not the UI.
- SQL tab → if the query never appears here either, confirm it failed during `explain()`/analysis, before execution.

## 🚨 Common Issues & Solutions

### Issue 1: "It says AnalysisException but my code looks fine"
**Symptom**: fails instantly, before any job runs; no entry in the Jobs/Stages tab.
**Root Cause**: this is a *plan-time* (Catalyst) error — a column/table/type problem caught during analysis, not during execution.
**Solution**: `df.printSchema()` and check names/case; the query never reached the cluster, so there's nothing to find in executor logs.

### Issue 2: The driver trace is generic ("Job aborted due to stage failure")
**Symptom**: no obvious cause on the driver; the message just says a stage failed after N attempts.
**Root Cause**: the driver only reports *that* a stage failed; the *why* lives on whichever executor produced the actual exception.
**Solution**: note the failing task/stage id and executor from the trace, then open that specific executor's stderr (UI → Executors, or `kubectl logs`) and read the last `Caused by:`.

### Issue 3: Different exceptions on each retry of the same task
**Symptom**: attempt 1 shows `FetchFailedException`, attempt 2 shows `ExecutorLostFailure`, attempt 3 times out.
**Root Cause**: this is usually a genuinely *transient* infrastructure issue (flaky node, memory pressure cascading into multiple symptoms), not a single deterministic bug (see Day 17 for the transient-vs-deterministic distinction).
**Solution**: correlate with node health (`kubectl get nodes`, `kubectl top nodes`) and cluster events around the failure window rather than chasing each exception message individually.

### Issue 4: Can't find the executor pod named in the trace
**Symptom**: the trace says "executor 5" but `kubectl get pods` doesn't show anything obviously matching.
**Root Cause**: Spark's internal executor id (a small integer) is not the same as the Kubernetes pod name; the pod name is usually `<app-name>-<id>-exec-<executor-id>`.
**Solution**: use the Executors tab in the Spark UI (or History Server) to map the internal executor id to its host/pod, or `kubectl get pods -l spark-role=executor,spark-app-selector=<app-id>` and match by start time.

### Issue 5: The job "succeeded" but ran much slower than normal
**Symptom**: no failure reported, but duration is 2-3x the usual baseline.
**Root Cause**: tasks were failing and silently succeeding on retry (within `spark.task.maxFailures`), each retry costing time, but the job as a whole passed.
**Solution**: check the Stages tab for any non-zero "Failed Tasks" even on a green job; the retries are still worth root-causing before they escalate into an outright failure.

## 📝 Key Takeaways
1. Five families cover almost everything: memory, shuffle, serialization, data, resource.
2. Read traces bottom-up to the last `Caused by:`.
3. The driver shows the symptom; the executor log shows the cause.
4. Plan-time errors (`AnalysisException`) never reach the cluster — no job, no stage, no executor log to chase.
5. One hypothesis, one change, one re-run.
6. On Kubernetes, `kubectl logs` (with `--previous`) + your aggregation stack are your primary tools; pods are ephemeral.
7. A "successful" job can still hide retries — check Failed Tasks even when the overall status is green.

## 🔗 Next Steps
- **Day 16**: OOM Debugging (Driver vs Executor)
- Practice: pull `kubectl logs` (and `--previous`) for one real failed job at work and classify it.
- Experiment: intentionally trigger a plan-time `AnalysisException` and a runtime data error side by side and compare what shows up (or doesn't) in the Spark UI.

## 📚 Additional Resources
- Spark Monitoring and Instrumentation docs
- Kubernetes logging architecture + your cluster's log-aggregation stack (Fluent Bit → Loki/EFK)
- `kubectl describe pod` and container termination reasons reference

---

**Progress**: Day 15/40 ✅
