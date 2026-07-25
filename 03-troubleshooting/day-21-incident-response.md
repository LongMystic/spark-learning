# Day 21: Production Incident Response

## 🎯 Learning Objectives
- Run a calm, repeatable playbook when a production Spark job breaks at 3am
- Triage: stabilize first, root-cause second
- Communicate status and decide mitigate-vs-fix
- Write a post-incident note that prevents the next occurrence

## 📚 Core Concepts

### 1. Incident vs bug
A production **incident** is "a pipeline the business depends on is failing or late *now*." The goal order is: **restore service → contain impact → find root cause → prevent recurrence.** Debugging deeply while the SLA burns is the classic mistake.

### 2. Severity triage
| Sev | Example | First move |
|-----|---------|-----------|
| 1 | Revenue/critical dashboard stale; job failing repeatedly | mitigate immediately, notify stakeholders |
| 2 | Non-critical pipeline late; retries ongoing | investigate, set expectation |
| 3 | Slow but succeeding; degraded | schedule a fix |

### 3. The playbook
```
1. ACKNOWLEDGE  — who's on it, what's impacted, since when.
2. ASSESS       — is it failing, hung, or slow? which stage? Spark UI / kubectl logs + kubectl get pods.
3. STABILIZE    — the fastest safe mitigation (below), even if ugly.
4. DIAGNOSE     — root cause via Days 15-20 method.
5. FIX          — permanent change + test.
6. POSTMORTEM   — timeline, cause, prevention. Blameless.
```

## 🔍 Deep Dive: Fast mitigations (buy time safely)

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

## 💡 Key Insights for On-Premise

### 1. Know your escape hatches before the incident
Have ready: `kubectl get pods` / `kubectl describe pod` for your namespace, the History Server URL, the `kubectl logs` command (and `--previous`), which namespace has spare quota, and who owns the upstream data. Finding these *during* Sev-1 wastes the SLA.

### 2. Shared-cluster incidents are often "someone else's job"
On a multi-tenant Kubernetes cluster, your job may fail because another team's namespace saturated shared node capacity or a node went NotReady. Check cluster-wide health (`kubectl get nodes`, `kubectl top nodes`, namespace quota utilization, Pending pods) before assuming your code broke.

## 🎯 Practical Exercises

### Exercise 1: Run the playbook on a scripted outage
```python
# See exercises/troubleshooting/exercise-21-incident-response.py
# A job fails; walk ACK->ASSESS->STABILIZE->DIAGNOSE and record each decision.
# Pair this with interview-prep/incident-drills.md.
```

### Exercise 2: Make a write idempotent
```python
# Convert a full-overwrite job to dynamic-partition-overwrite so re-runs are safe.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. **SLA / freshness** of the output table (is it late?).
2. **Job success rate & retry count** over time (flapping?).
3. **Cluster queue utilization** (starvation).

### Spark UI Analysis
- History server: compare today's failed run to yesterday's healthy run (data volume, stage times) to spot what changed.

## 🚨 Common Issues & Solutions

### Issue 1: Re-run produced duplicate rows
**Symptom**: mitigation by re-run corrupted the table.
**Solution**: writes weren't idempotent — use dynamic overwrite or MERGE; clean up, then re-run.

### Issue 2: "Fixed" it by bumping memory, broke again next week
**Symptom**: recurring incident.
**Solution**: you mitigated, not root-caused. Data grew / skew returned — address the cause and add a data-volume alert.

## 📝 Key Takeaways
1. Restore service first; deep-debug second.
2. Triage by severity; communicate early.
3. Idempotent writes make re-run the safest mitigation.
4. Check cluster/queue health — it may not be your job.
5. Blameless postmortems with concrete prevention close the loop.

## 🔗 Next Steps
- **Phase 3 complete** → take [assessments/phase-3-assessment.md](../assessments/phase-3-assessment.md)
- **Day 22**: Custom Optimizations & Catalyst Rules (Phase 4)

## 📚 Additional Resources
- SRE incident-management practices (blameless postmortems)
- Your org's on-call runbook

---

**Progress**: Day 21/40 ✅
