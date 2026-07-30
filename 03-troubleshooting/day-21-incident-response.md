# Day 21: Production Incident Response

## 🎯 Learning Objectives
- Run a calm, repeatable playbook when a production Spark job breaks at 3am
- Triage: stabilize first, root-cause second
- Communicate status and decide mitigate-vs-fix
- Write a post-incident note that prevents the next occurrence
- Build the on-premise Kubernetes "escape hatches" you need ready *before* an incident starts

## 📚 Core Concepts

### 1. Incident vs bug

A production **incident** is "a pipeline the business depends on is failing or late *now*." The goal order is: **restore service → contain impact → find root cause → prevent recurrence.** Debugging deeply while the SLA burns is the classic mistake.

**Key Points:**
- A bug is something you can take your time investigating; an incident has a clock running against it (an SLA, a downstream dashboard, a dependent team's own pipeline).
- The instinct to "just find the root cause first" is usually wrong under incident conditions — a fast, safe mitigation buys you the time to root-cause properly without the SLA pressure clouding your judgment.
- Every technique from Days 15-20 (reading traces, classifying OOM, diagnosing FetchFailed, performance triage) is still exactly how you *diagnose* during an incident — this lesson is about the process wrapped around that diagnosis, not a replacement for it.

### 2. Severity triage

| Sev | Example | First move |
|-----|---------|-----------|
| 1 | Revenue/critical dashboard stale; job failing repeatedly | mitigate immediately, notify stakeholders |
| 2 | Non-critical pipeline late; retries ongoing | investigate, set expectation |
| 3 | Slow but succeeding; degraded | schedule a fix |

**Key Points:**
- Severity determines *urgency of communication*, not just urgency of technical response — a Sev-1 needs stakeholders told within minutes, even before you know the cause.
- Don't over-escalate a Sev-3 into a fire drill, and don't under-escalate a Sev-1 by quietly debugging it alone — both waste organizational trust.
- Re-assess severity as you learn more: a job that looked like a quick retry (Sev-2) that's now failed three times in a row with the SLA approaching should be re-triaged to Sev-1.

### 3. The playbook
```
1. ACKNOWLEDGE  — who's on it, what's impacted, since when.
2. ASSESS       — is it failing, hung, or slow? which stage? Spark UI / kubectl logs + kubectl get pods.
3. STABILIZE    — the fastest safe mitigation (below), even if ugly.
4. DIAGNOSE     — root cause via Days 15-20 method.
5. FIX          — permanent change + test.
6. POSTMORTEM   — timeline, cause, prevention. Blameless.
```

**Key Points:**
- **ACKNOWLEDGE** is intentionally the first step, even before you've looked at a single log — it stops multiple people from silently duplicating investigation and starts the stakeholder clock.
- **ASSESS** is a fast triage (minutes, not an hour) using exactly the classification skills from Day 15: failing, hung (stuck `Pending`/no progress), or slow (running but past its usual duration).
- **STABILIZE** is deliberately separated from **DIAGNOSE** — you are allowed to apply an "ugly" fix (re-run, quarantine bad data, borrow quota) that buys time without understanding the full root cause yet.
- **POSTMORTEM** exists specifically to prevent the same incident from recurring — a postmortem that just says "we re-ran it and it worked" without addressing why it broke has failed at its one job.

### 4. What a good postmortem contains

**Key Points:**
- **Timeline** — timestamped facts only ("02:47 alert fired", "02:55 mitigated", "09:25 root cause confirmed"), not interpretation or blame woven into the timeline itself.
- **Impact** — which table(s)/dashboard(s)/downstream consumers were affected, for how long, and how stale the data got.
- **Root cause** — the actual technical cause (e.g. "undersized shuffle partitions caused executor OOM under this month's higher data volume, cascading into FetchFailed"), stated precisely enough that someone unfamiliar with the incident could understand what broke and why.
- **Prevention items** — concrete, assignable, checkable actions (e.g. "add a Prometheus alert when namespace ResourceQuota memory utilization exceeds 90%"), each with an owner and a target date — not vague intentions.
- **What went well / what didn't** in the response itself (not just the technical cause) — e.g. "MTTA was 2 minutes, good; but the runbook didn't have the History Server URL, costing 10 minutes."

## 🔍 Deep Dive: Fast mitigations (buy time safely)

### Step-by-Step Process for choosing a mitigation
1. **Classify the failure** using Day 15's error families (memory, shuffle, serialization, data, resource) or Day 20's performance categories (skew, spill, I/O, join) — even a 60-second classification narrows your mitigation options.
2. **Ask: is this transient or will it recur immediately on re-run?** (Day 17's transient-vs-deterministic lens applies directly here.)
3. **Check cluster-wide health first** — `kubectl get nodes`, namespace `ResourceQuota` usage, and Pending pods — because "someone else's job" is a common cause on a shared cluster (see Key Insights below).
4. **Pick the least invasive mitigation that unblocks the SLA**, from the list below.
5. **Confirm the mitigation actually worked** (job completed, table updated, downstream consumer unblocked) before declaring stabilized.

### Mitigation options, matched to cause
- **Transient/infra flake** → re-run the job (idempotent writes make this safe — see below).
- **Resource starvation** (namespace quota exhausted / pods stuck `Pending`) → resubmit into a namespace with free quota, raise the `ResourceQuota` temporarily, or bump executors.
- **Skew blew up today** → enable AQE skew join / raise `shuffle.partitions` and re-run.
- **Bad input data** (upstream sent garbage) → quarantine the bad partition, re-run for good partitions, escalate upstream.
- **Downstream deadline** → deliver yesterday's snapshot / partial data with a clear caveat while you fix.

### Idempotency makes re-runs safe
```python
# Dynamic partition overwrite: re-running only rewrites the affected date, not the table.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
(df.write.mode("overwrite").partitionBy("txn_date").parquet("out/"))
# Or MERGE/upsert with Iceberg (Day 34) for exactly-once semantics.
```

### Example: working a Sev-1 end to end

```
02:47 — Alert: daily-etl DAG task failed in Airflow, 3rd consecutive attempt.
02:49 — ACKNOWLEDGE: on-call posts in #data-incidents: "daily-etl failing since
         02:10, investigating, table last updated yesterday's data (stale)."
02:52 — ASSESS: kubectl get pods -n spark-jobs shows the driver pod
         Error/CrashLoopBackOff; kubectl logs --previous shows FetchFailedException;
         Executors tab (via History Server, since pods are gone) shows executor 9
         removed shortly before the fetch failure.
02:55 — STABILIZE: check kubectl describe resourcequota -n spark-jobs -- namespace
         is near its memory cap because a batch job from another team is also
         running. Re-run in a namespace with spare quota / request a temporary
         quota bump; mitigate the SLA.
03:20 — Job completes with the temporary quota bump. Table is fresh. Stakeholders
         notified: "daily-etl recovered at 03:20, investigating root cause."
03:25 — DIAGNOSE: the underlying cause was memory pressure on shared nodes causing
         an executor OOM (Day 16) which cascaded into FetchFailed (Day 18).
09:00 — FIX: right-size shuffle partitions for this job's current data volume,
         and open a ticket with the platform team about the namespace's
         ResourceQuota being sized for last quarter's data volume.
09:30 — POSTMORTEM: blameless write-up, timeline, root cause (undersized
         partitions + tight quota), and two concrete preventions (partition
         resize done today; quota review scheduled).
```

**Analysis:**
- Notice how the mitigation (quota bump / re-run) happened *before* the full root cause (executor OOM cascading to FetchFailed) was even confirmed — that's the point of separating STABILIZE from DIAGNOSE.
- The postmortem produced two concrete, checkable prevention items, not a vague "we'll be more careful" — that's what makes a postmortem blameless *and* useful.
- Idempotent writes (dynamic partition overwrite) meant the mitigating re-run carried zero risk of corrupting the table, which is exactly why idempotency is worth setting up *before* an incident, not during one.

### Example: a Sev-2 that should have stayed Sev-2 (and almost didn't)

```
14:10 — Airflow marks daily-etl as "up_for_retry" (attempt 1 of 3 failed).
14:12 — On-call checks: this pipeline's SLA is "by 18:00", and Airflow retries
         automatically with backoff. ASSESS: single transient FetchFailed,
         Executors tab shows one executor lost around 14:05 -- looks like an
         isolated node blip, not a pattern.
14:12 — Classified Sev-2: investigate, set expectation, no stakeholder page yet.
14:14 — Post a low-key note in #data-pipelines: "daily-etl retry 1 failed
         (transient), Airflow will retry automatically, SLA is 18:00, not
         currently at risk."
14:35 — Attempt 2 succeeds. Table updated on time. Close out the note.
14:40 — Light-touch root cause: check the lost executor's node in kubectl
         get events -- confirms a one-off kubelet restart, not a recurring
         issue. No further action needed; no full postmortem required for a
         Sev-2 that self-resolved within its SLA and had a clear, one-off cause.
```

**Analysis:**
- The key judgment call was recognizing this as genuinely Sev-2 (SLA not at risk, automatic retry available, isolated cause) rather than over-escalating a single transient failure into a full incident response.
- Not every Sev-2/3 needs a full blameless postmortem — but it's still worth a brief note on *why* it resolved, so a pattern across several "minor" events isn't missed later.
- Contrast this with the Sev-1 example above: the difference wasn't the exception type (`FetchFailedException` in both cases) — it was the SLA proximity, whether the cause looked isolated or systemic, and whether an automatic retry was already in flight.

## 💡 Key Insights for On-Premise

### 1. Know your escape hatches before the incident
Have ready: `kubectl get pods` / `kubectl describe pod` for your namespace, the History Server URL, the `kubectl logs` command (and `--previous`), which namespace has spare quota, and who owns the upstream data. Finding these *during* Sev-1 wastes the SLA.

```bash
# Keep these commands and URLs bookmarked/runbooked for every pipeline you own:
kubectl -n spark-jobs get pods -l sparkoperator.k8s.io/app-name=<app>
kubectl -n spark-jobs describe resourcequota
kubectl -n spark-jobs logs <driver-pod> --previous
# History Server (survives after pods are gone, reads s3a://spark-events):
#   http://spark-history:18080
# Airflow DAG run + task logs for the orchestration layer:
#   airflow dags list-runs -d <dag_id>
```

### 2. Shared-cluster incidents are often "someone else's job"
On a multi-tenant Kubernetes cluster, your job may fail because another team's namespace saturated shared node capacity or a node went NotReady. Check cluster-wide health (`kubectl get nodes`, `kubectl top nodes`, namespace quota utilization, Pending pods) before assuming your code broke.

### 3. Airflow is part of your incident surface
Because these pipelines are orchestrated by Airflow, an "incident" often first appears as a failed/late Airflow task, not directly as a Spark UI failure. Know how to pull the Airflow task log and how a SparkApplication CRD's status maps back to the Airflow task state (e.g. via a `SparkKubernetesOperator` sensor) — the first ASSESS step often starts in Airflow, not `kubectl`.

### 4. Have a communication template ready
A short, pre-agreed status-update format ("what's impacted / since when / current mitigation / next update time") posted to a shared channel saves time during a real incident and sets consistent stakeholder expectations, rather than improvising the wording under pressure.

## 🎯 Practical Exercises

### Exercise 1: Run the playbook on a scripted outage
```python
# See exercises/troubleshooting/exercise-21-incident-response.py
# Also builds and verifies an idempotent dynamic-partition-overwrite write,
# then walks the ACK -> ASSESS -> STABILIZE -> DIAGNOSE -> FIX -> POSTMORTEM
# playbook against a scripted description of an outage. Record each decision
# in writing, not just mentally -- that's the actual skill being practiced.
# Pair this with interview-prep/incident-drills.md.
```

### Exercise 2: Make a write idempotent
```python
# Convert a full-overwrite job to dynamic-partition-overwrite so re-runs are safe.
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
daily = txns.groupBy("txn_date", "category").agg(F.sum("amount").alias("total"))
daily.write.mode("overwrite").partitionBy("txn_date").parquet(out)
# Run it twice in a row and assert row counts match -- a re-run that
# silently duplicates or drops rows is not a safe mitigation.
count1 = spark.read.parquet(out).count()
daily.write.mode("overwrite").partitionBy("txn_date").parquet(out)
count2 = spark.read.parquet(out).count()
assert count1 == count2, "Write was NOT idempotent!"
```

### Exercise 3: Build your own runbook
```markdown
# Write a one-page runbook for a real pipeline you own, including:
# - kubectl commands to check pod/quota/node status for its namespace
# - the History Server URL and how to find this job's app id
# - who owns the upstream data source, and how to reach them
# - the safe mitigation for its most likely failure mode (from Days 15-20)
# - whether its writes are idempotent today, and if not, what to fix first
```

### Exercise 4: Triage severity for three scenarios
```markdown
For each scenario, decide Sev 1/2/3 and the first move, using the table in
Core Concepts section 2:
  A) The executive revenue dashboard's source table is 6 hours stale and
     the job has failed 3 times in a row.
  B) A non-critical enrichment pipeline is running 40 minutes behind its
     usual schedule but is still progressing (no failures).
  C) A job succeeds every run but has been slowly creeping up in duration
     over the past two weeks, still well within its SLA.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. **SLA / freshness** of the output table (is it late?).
2. **Job success rate & retry count** over time (flapping?).
3. **Cluster queue utilization** (starvation).
4. **Mean time to acknowledge (MTTA) and mean time to mitigate (MTTM)** — track these separately from time-to-root-cause; they measure whether the playbook itself is working.
5. **Recurrence rate** of the same root cause across postmortems — a rising trend means prevention items aren't actually closing the loop.

### Spark UI Analysis
- History server: compare today's failed run to yesterday's healthy run (data volume, stage times) to spot what changed.
- For a job that failed partway, the History Server's stage list shows exactly which stage was in progress at failure time — pair this with the Airflow task log's timestamp to build the incident timeline for the postmortem.

## 🚨 Common Issues & Solutions

### Issue 1: Re-run produced duplicate rows
**Symptom**: mitigation by re-run corrupted the table.
**Root Cause**: writes weren't idempotent — a full overwrite or a plain append re-run duplicated or dropped data.
**Solution**: use dynamic overwrite or MERGE; clean up, then re-run. Fix idempotency *before* the next incident, not during this one.

### Issue 2: "Fixed" it by bumping memory, broke again next week
**Symptom**: recurring incident.
**Root Cause**: you mitigated, not root-caused — data grew or skew returned, and the memory bump only bought temporary headroom.
**Solution**: address the cause and add a data-volume alert so growth is caught before it becomes an incident again.

### Issue 3: Mitigation was applied but nobody updated stakeholders
**Symptom**: the job actually recovered, but downstream teams kept escalating because they never heard it was fixed.
**Root Cause**: STABILIZE happened silently — the communication step of ACKNOWLEDGE was treated as a one-time action instead of an ongoing thread.
**Solution**: post a close-out update the moment the mitigation is confirmed working, and again once the permanent fix ships — don't let stakeholders find out via the dashboard turning green on its own.

### Issue 4: Postmortem blames a person or team
**Symptom**: the write-up reads as "X forgot to do Y" rather than "the system allowed Y to go unnoticed."
**Root Cause**: postmortems drift toward blame when they focus on the individual action instead of the process/tooling gap that allowed it.
**Solution**: reframe every postmortem finding as a system gap with a concrete, assignable prevention item — e.g. not "the engineer didn't check quota" but "no alert exists for namespace quota approaching its limit; add one."

### Issue 5: The same root cause keeps reappearing across incidents
**Symptom**: postmortems for three separate incidents this quarter all trace back to the same undersized `ResourceQuota` or the same lack of data validation.
**Root Cause**: prevention items from earlier postmortems were logged but never actually completed, or were too vague to action.
**Solution**: track postmortem action items to closure like any other work item, with an owner and a deadline, not just a bullet point in a document nobody revisits.

## 📝 Key Takeaways
1. Restore service first; deep-debug second.
2. Triage by severity; communicate early.
3. Idempotent writes make re-run the safest mitigation.
4. Check cluster/queue health — it may not be your job.
5. Blameless postmortems with concrete prevention close the loop.
6. Know your escape hatches (kubectl commands, History Server, quota owners) before an incident, not during one.
7. Track prevention items to closure, or the same incident will recur.

## 🔗 Next Steps
- **Phase 3 complete** → take [assessments/phase-3-assessment.md](../assessments/phase-3-assessment.md)
- **Day 22**: Custom Optimizations & Catalyst Rules (Phase 4)

## 📚 Additional Resources
- SRE incident-management practices (blameless postmortems)
- Your org's on-call runbook
- Kubernetes `ResourceQuota` and node health reference (`kubectl get nodes`, `kubectl top nodes`)

---

**Progress**: Day 21/40 ✅
