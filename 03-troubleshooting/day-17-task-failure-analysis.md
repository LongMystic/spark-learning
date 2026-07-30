# Day 17: Task Failure & Retry Analysis

## 🎯 Learning Objectives
- Understand Spark's task → stage → job retry model and its config knobs
- Tell transient failures (retry helps) from deterministic ones (retry just wastes time)
- Diagnose stragglers and decide when speculation helps vs hurts
- Read the "N failed / M succeeded" task picture in the Spark UI
- Use node/executor exclusion to contain a single bad node's blast radius

## 📚 Core Concepts

### 1. The retry hierarchy

Spark's fault tolerance is built on retrying at three levels, each with its own limit and its own cost:

```
Task attempt  --(fails)-->  retried up to spark.task.maxFailures (default 4)
Stage         --(FetchFailed)-->  retried up to spark.stage.maxConsecutiveAttempts (default 4)
Job           --(stage exhausts retries)-->  fails the whole job
```

**Key Points:**
- **`spark.task.maxFailures = 4`**: a single task can fail 3 times and still let the job pass on the 4th. This is why a job can succeed with zero visible errors to the end user while quietly having failed-and-retried several tasks.
- A stage failure caused by `FetchFailedException` re-runs the *parent* (map) stage to regenerate lost shuffle files — expensive, because it recomputes work that had already finished.
- `spark.stage.maxConsecutiveAttempts` (default 4) caps how many times Spark will retry an entire stage before giving up on the job; each retry re-executes every task in that stage, not just the one that originally failed.
- Task retries happen on a **different executor** than the one that failed, when possible — Spark deliberately avoids retrying on the same host to route around a bad node.

**Example:**
```
# Reading a UI task table:
Index  Attempt  Status   Duration  Executor
  45      0     FAILED   1.2min    exec-3   <- first attempt failed
  45      1     FAILED   1.1min    exec-7   <- retried on a different executor
  45      2     FAILED   1.3min    exec-2
  45      3     SUCCESS  1.2min    exec-5   <- succeeded on the 4th attempt

# Same index, same error every time -> almost certainly deterministic (a data/code bug),
# NOT a flaky executor -- Spark already tried three different executors.
```

### 2. Transient vs deterministic

This is the single most important judgment call in this lesson: does retrying even have a chance of fixing the problem?

| Transient (retry helps) | Deterministic (retry is futile) |
|-------------------------|----------------------------------|
| Executor lost / node hiccup | `NullPointerException` in a UDF on a specific row |
| Network blip, `FetchFailed` | Bad cast / parse on specific data |
| Preempted / evicted pod | Divide-by-zero, schema mismatch |
| Transient disk-full | Non-serializable closure |

**Key Points:**
- If the **same task index** fails all 4 attempts with the **same exception**, it's deterministic — a data or code bug, not infrastructure.
- If each attempt fails with a *different* exception (timeout, then FetchFailed, then lost executor), that's a strong transient-infrastructure signal — a genuinely unstable environment, not a code bug.
- Deterministic failures are actually *good news* once identified — they're reproducible, so you can isolate the exact input and fix it, unlike transient issues which require broader infrastructure investigation.
- The task attempt landing on three or four **different executors** and still failing the same way rules out "one bad node" as the explanation — it points straight at the data or the code.

### 3. Speculation (stragglers)

`spark.speculation=true` relaunches slow task copies and takes whichever finishes first.

**Key Points:**
- **Helps**: a slow/failing node makes a few tasks lag — a speculative copy on a healthy node finishes first and the slow one is killed.
- **Hurts**: skew (the slow task is slow because it has 10× the data — a duplicate is equally slow, wasting resources for no gain) and non-idempotent writes (duplicate output if both copies happen to both complete and both write).
- Speculation has real cost: it doubles resource usage for the speculated tasks. On a resource-constrained on-premise cluster with tight namespace quotas, that's not free.
- Key thresholds: `spark.speculation.quantile` (fraction of tasks that must complete before speculation kicks in, default 0.75) and `spark.speculation.multiplier` (how much slower than the median a task must be to be speculated, default 1.5).

**Example:**
```bash
--conf spark.speculation=true
--conf spark.speculation.quantile=0.9      # wait until 90% of tasks finish first
--conf spark.speculation.multiplier=2      # only speculate tasks 2x slower than median
```

### 4. Locality and retries interact

Each task retry attempts to schedule on a **different** executor when Spark has that option, which affects how you read a retry pattern.

**Key Points:**
- If attempts 1-4 ran on 4 different executors and all failed identically, you've effectively ruled out "one bad executor" — Spark already spread the attempts across the cluster for you.
- If Spark *couldn't* schedule elsewhere (e.g. a tiny cluster, or data locality constraints pin the task near its input), you might see two attempts land on the same host — don't over-read that as confirmation of a bad node with only two data points.
- `spark.locality.wait` (default 3s) controls how long the scheduler waits for a preferred (data-local) placement before falling back to a less-local one; this indirectly affects how quickly a retry gets rescheduled after a failure.

## 🔍 Deep Dive: Diagnosing a failing task

### Step-by-Step Process
1. UI → Stages → failed stage → sort tasks by **Status** and **Duration**.
2. Is it **the same task index** failing repeatedly? → deterministic; open its input partition.
3. Different tasks failing on the **same executor/host**? → bad node; blacklist/decommission.
4. `FetchFailed` on a stage? → a map-side executor died; the stage re-runs. Fix the *executor loss* (Day 16/18), not the reduce stage.
5. Check the task's **input split** — which file/partition? Reproduce locally on just that slice.
6. Once reproduced, decide: is this a data problem (bad row, needs validation/quarantine) or a code problem (UDF doesn't handle an edge case)?

### Example: end-to-end diagnosis of a deterministic failure

```python
# Symptom from the UI: "Task 88 in stage 14.0 failed 4 times" — same
# ValueError each time, on executors 2, 5, 7, and 3 in turn (all different).

# Step 1: identify the input split for task 88 (Stages -> task table -> "Locality Level"
# and input size/records columns narrow down which file/partition it read).

# Step 2: narrow to the offending partition/file, then to the row
suspect = spark.read.parquet("data/transactions").where("txn_date = '2026-07-01'")
suspect.where("amount IS NULL OR quantity = 0").show(truncate=False)

# Step 3: reproduce the UDF failure directly, outside the full job
from pyspark.sql.functions import col, udf

@udf("int")
def explode_on_zero_mod(x):
    if x % 500 == 0:
        raise ValueError(f"boom on {x}")
    return int(x)

suspect.withColumn("v", explode_on_zero_mod(col("txn_id"))).collect()
# Reproduces on the single slice -> confirmed deterministic, confirmed root cause.
```

**Analysis:**
- Because the four attempts landed on four *different* executors and still failed identically, infrastructure is ruled out — this is a data/code bug.
- Narrowing to the exact partition first (rather than debugging the whole table) turns a multi-minute cluster job into a sub-second local repro.
- Once reproduced, the fix is either a data-quality fix upstream (bad rows shouldn't exist) or a defensive UDF (handle the edge case, e.g. via `try/except` inside the UDF or a `when()`/`otherwise()` guard before it).

### Reproducing a deterministic row failure
```python
# Narrow to the offending partition/file, then to the row
suspect = spark.read.parquet("data/transactions").where("txn_date = '2026-07-01'")
suspect.where("amount IS NULL OR quantity = 0").show(truncate=False)
```

### Example: a transient failure that looks alarming but isn't a bug

```
Task 30 in stage 6.0 (TID 812) (10.42.1.9 executor 3): TaskKilled (another attempt succeeded)
Task 31 in stage 6.0 (TID 813) (10.42.1.9 executor 3): ExecutorLostFailure (executor 3 exited caused by one of the running tasks)
Reason: Remote RPC client disassociated. Likely due to containers exceeding
        thresholds, or network issues.
```
Cross-checking `kubectl`:
```bash
$ kubectl -n spark-jobs get pods -l spark-role=executor
daily-etl-abc-exec-3   0/1   Evicted   0   22m
$ kubectl -n spark-jobs describe pod daily-etl-abc-exec-3
Status: Failed
Reason: Evicted
Message: The node was low on resource: memory. Container exec was using 5100Ki, which exceeds its request.
```

**Analysis:**
- `TaskKilled (another attempt succeeded)` is not itself a failure worth investigating — it's Spark cleaning up a redundant attempt (either speculative or from a stage retry) once another copy already finished.
- `ExecutorLostFailure` with "Remote RPC client disassociated" is generic — it says the connection dropped, not *why*. The real cause is in `kubectl describe pod`: the node itself was under memory pressure and evicted the pod, which is a node-level event, not something the executor's own JVM logs would show clearly.
- Because this is node-level eviction rather than a deterministic task/data bug, the correct response is **not** to inspect the task's input data — it's to check node health cluster-wide (`kubectl top nodes`, `kubectl describe node`) and consider whether the node's other resident pods (yours or another team's) are oversubscribing it.

## 💡 Key Insights for On-Premise

### 1. Blacklisting / exclusion
`spark.excludeOnFailure.enabled=true` (a.k.a. blacklisting) stops scheduling on a node that keeps failing tasks — invaluable when one worker node has a bad disk. Exclusion still works **per-node** on Kubernetes. On shared clusters, one flaky node can fail many jobs; exclusion contains the blast radius, and the platform team can also `cordon`/`taint` the bad node so the scheduler stops placing any pods there.

```bash
--conf spark.excludeOnFailure.enabled=true
--conf spark.excludeOnFailure.task.maxTaskAttemptsPerNode=2
--conf spark.excludeOnFailure.stage.maxFailedTasksPerExecutor=2
```

### 2. Don't mask bugs with retries
Raising `spark.task.maxFailures` to 16 to "get past" a failure usually just delays a deterministic error by 4×, burning cluster time and quota on a namespace shared with other teams. Fix the row/code instead.

### 3. Cross-check with `kubectl` node health
Spark's own exclusion mechanism works at the Spark scheduler level, but on a multi-tenant Kubernetes cluster it's worth confirming with `kubectl get nodes` / `kubectl describe node <node>` whether the "bad executor" pattern correlates with a node already flagged `NotReady` or under disk/memory pressure — that's useful signal to hand to the platform team, beyond what Spark's own exclusion can see.

### 4. Speculation and shared quota
Because speculative task copies consume additional executor slots, running speculation with a tight `ResourceQuota` in your namespace can itself cause other tasks to queue. Size speculation's aggressiveness (`multiplier`, `quantile`) with your namespace's actual spare capacity in mind.

## 🎯 Practical Exercises

### Exercise 1: Deterministic vs transient
```python
# See exercises/troubleshooting/exercise-17-task-failures.py
# A UDF throws on a specific value (x % 500 == 0); with maxFailures lowered
# to 2, observe both attempts fail identically -- deterministic. Practice
# stating WHY it's deterministic (same task, same exception, different
# executors) rather than just observing the failure.
```

### Exercise 2: Straggler simulation
```python
# Create a skewed partition; enable speculation; observe duplicate slow tasks
# and confirm they do NOT finish any faster -- because the duplicate has the
# same oversized data as the original. Then disable speculation and instead
# apply an AQE skew join / salting fix and compare stage time.
spark.conf.set("spark.speculation", "true")
spark.conf.set("spark.speculation.multiplier", "1.5")
# ... run the skewed groupBy from Day 10/18 ...
# Compare against:
spark.conf.set("spark.speculation", "false")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
```

### Exercise 3: Exclude a bad executor
```bash
# Enable exclusion and lower its thresholds so it reacts fast in a test run:
spark-submit \
  --conf spark.excludeOnFailure.enabled=true \
  --conf spark.excludeOnFailure.task.maxTaskAttemptsPerNode=1 \
  your_job.py
# Observe in the Executors tab that a consistently failing executor is
# excluded from future task scheduling within the same application.
```

### Exercise 4: Tell node eviction from a data bug
```bash
# Given a task failure with "ExecutorLostFailure ... Remote RPC client
# disassociated", cross-check the node, not just the task:
kubectl -n spark-jobs get pods -o wide | grep exec
kubectl -n spark-jobs describe pod <the-lost-executor-pod>
kubectl top nodes
# If the pod shows Evicted / node memory pressure, this is transient and
# environmental -- re-running (or excluding the bad node) is the correct
# response, NOT inspecting the task's input partition for a data bug.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Failed Tasks** and **task attempt numbers** (`.0`, `.1`, `.2`, `.3`).
2. **Per-host failure counts** (bad-node signal).
3. **Duration distribution** (min/median/max) → stragglers/skew.
4. **Speculative task count** (Stages tab task table, "Speculative" status) — a rising count without corresponding speedups signals speculation is fighting skew rather than a slow node.
5. **Stage retry count** in the Stages tab (stage id shows as `8.0`, `8.1`, `8.2` for retries) — a climbing count means the underlying cause (usually executor loss) is unresolved.

### Spark UI Analysis
- Stage page "Aggregated Metrics by Executor" reveals a single bad executor.
- Task table "Errors" column groups the repeated exception.
- Sorting the task table by "Duration" descending immediately surfaces stragglers; sorting by "Status" groups all failures together for a fast same-exception check.
- The stage's attempt number (visible as `Stage Id` `N.M`) tells you at a glance how many times the whole stage has already been retried.

## 🚨 Common Issues & Solutions

### Issue 1: Job fails after ~4 identical task errors
**Symptom**: "Task X in stage Y failed 4 times."
**Root Cause**: the same task index fails identically on every attempt, across different executors — a data or code bug, not infrastructure.
**Solution**: deterministic — inspect that task's data/UDF; retries won't help. Narrow to the exact input partition and reproduce locally.

### Issue 2: Speculation makes things worse
**Symptom**: duplicate tasks, more load, no speedup.
**Root Cause**: the lag is skew (a task with far more data than its peers), not a slow node — a speculative duplicate has the identical oversized data and is equally slow.
**Solution**: fix skew (salting/AQE), disable speculation for that job.

### Issue 3: Each attempt of the same task fails with a different exception
**Symptom**: attempt 1 times out, attempt 2 shows `FetchFailedException`, attempt 3 shows executor lost.
**Root Cause**: genuine environmental instability — a flaky node, a cluster-wide resource crunch, or network issues — rather than a single reproducible bug.
**Solution**: correlate failure timestamps with `kubectl get events`/node health across the cluster; consider enabling exclusion so the scheduler routes around the worst-behaving node while the platform team investigates.

### Issue 4: A whole stage keeps re-running from scratch
**Symptom**: stage id increments (`8.0` → `8.1` → `8.2`) and each retry re-executes every task, not just the one that failed.
**Root Cause**: a `FetchFailedException` invalidates the map-side shuffle output, forcing Spark to recompute the entire parent stage, not just the failed reduce task.
**Solution**: this is a shuffle/executor-loss problem, not a task-retry problem — see Day 18 for the underlying `FetchFailed` root cause and Day 16 for the executor loss itself.

### Issue 5: Raising `maxFailures` "fixed" the job, but it's slow every run
**Symptom**: the job now passes after bumping `spark.task.maxFailures` to a high number, but takes much longer than before.
**Root Cause**: the underlying deterministic (or semi-deterministic) failure still happens every run; you're paying the cost of several failed attempts per affected task every single time, just under a higher ceiling.
**Solution**: this is masking, not fixing. Identify and correct the actual row/code issue; treat any `maxFailures` increase as temporary triage, never a permanent setting.

## 📝 Key Takeaways
1. `spark.task.maxFailures=4` — same task + same error 4× ⇒ deterministic bug.
2. `FetchFailed` re-runs the parent map stage; fix the executor loss upstream.
3. Speculation cures slow nodes, not skew or non-idempotent writes.
4. Exclusion/blacklisting contains one bad node.
5. Reproduce deterministic failures on the single offending partition.
6. Different exceptions across attempts of the same task point to environment instability, not a code bug.
7. Never leave a raised `maxFailures` as the permanent fix — it hides the real problem and costs time on every run.

## 🔗 Next Steps
- **Day 18**: Shuffle Error Resolution (FetchFailed)
- Practice: find a stage with failed tasks and determine transient vs deterministic.
- Experiment: enable exclusion (`spark.excludeOnFailure.enabled=true`) on a job with a known bad node and confirm the scheduler stops placing tasks there.

## 📚 Additional Resources
- Spark configuration: task/stage retry, speculation, exclusion
- Spark scheduler internals: task scheduling and locality

---

**Progress**: Day 17/40 ✅
