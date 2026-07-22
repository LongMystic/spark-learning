# Day 39: Architecture, Multi-Tenancy, HA & Security

## 🎯 Learning Objectives
- Choose batch/streaming architectures (lambda vs kappa) sensibly
- Share a cluster fairly across teams (YARN queues, scheduler pools)
- Design for high availability and graceful failure
- Apply on-prem security basics (Kerberos, authorization, encryption)

## 📚 Core Concepts

### 1. Batch, streaming, or both
| Pattern | Idea | Cost |
|---------|------|------|
| **Batch-only** | scheduled jobs (Airflow) | simplest; latency = schedule |
| **Lambda** | batch + speed layer, merged at query | powerful but **two codebases** to keep in sync |
| **Kappa** | one streaming pipeline; reprocess by replaying the log | one codebase; needs a durable log (Kafka) |
Most on-prem shops are batch-first and add streaming (Days 30–31) only where latency truly matters. Don't adopt lambda's complexity without the requirement.

### 2. Multi-tenancy on YARN
Shared clusters need **isolation** so one team can't starve another:
- **Capacity/Fair scheduler queues** with guaranteed + max capacity per team.
- **Spark fair-scheduler pools** within an app (e.g. the Thrift Server, Day 29).
- **Dynamic allocation + external shuffle service** so idle resources return to the pool.
- **Per-queue limits** on max executors / cores to cap blast radius.

### 3. High availability
- **YARN RM HA** and **NameNode HA** (cluster-side) so the platform survives a node loss.
- **Checkpointing** for streaming (Days 30–31) → recover exactly-once.
- **Idempotent, retryable jobs** (Days 21, 35, 38) → automatic recovery from transient failure.
- **Thrift Server HA** → supervised + load-balanced (Day 29).

## 🔍 Deep Dive: Security on-prem
- **Authentication**: **Kerberos** is the norm — jobs authenticate with keytabs/principals; long-running apps need delegation-token renewal (`--principal`/`--keytab`).
- **Authorization**: Ranger/Sentry or storage ACLs control table/column/row access; Iceberg/Hive integrate with these.
- **Encryption**: in transit (RPC/shuffle/UI TLS: `spark.ssl.*`, `spark.network.crypto.enabled`) and at rest (HDFS encryption zones).
- **Isolation**: run per-tenant on separate queues; avoid sharing one credential across teams.

## 💡 Key Insights for On-Premise
### 1. Right-size at the platform, not per job
On a shared cluster, a job asking for 500 executors just moves a bottleneck. Set sane per-queue caps and let dynamic allocation flex within them; coordinate big jobs with the platform team.

### 2. Design for the node that will die
Assume any executor/node can vanish. Everything you learned — external shuffle service, checkpoints, idempotent writes, retries — exists so a single failure is a hiccup, not an incident.

### 3. Kerberos token expiry kills long jobs
Streaming/Thrift apps that run for days fail when tokens expire unless you configure keytab-based renewal. This is a classic on-prem long-running-job gotcha.

## 🎯 Practical Exercises

### Exercise 1: Reason about a shared-cluster design
```python
# See exercises/production/exercise-39-architecture.py
# Given 3 teams + BI + streaming, propose queue capacities and isolation; print the config.
```

### Exercise 2: Failure-mode checklist
```python
# For each component (driver, executor, RM, STS, Kafka), state the HA/recovery mechanism.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Per-queue utilization & pending containers (starvation).
2. Job success rate across tenants.
3. Token-renewal / auth failures on long-running apps.

## 🚨 Common Issues & Solutions

### Issue 1: One team starves the cluster
**Symptom**: others' jobs pend forever.
**Solution**: queue capacities + max-executor caps + fair pools.

### Issue 2: Long-running app dies after N hours
**Symptom**: auth failure mid-run.
**Solution**: Kerberos keytab renewal (`--principal`/`--keytab`).

## 📝 Key Takeaways
1. Prefer batch-first; add streaming/lambda/kappa only when justified.
2. Isolate tenants with queues + fair pools + dynamic allocation.
3. HA = RM/NN HA + checkpoints + idempotent retryable jobs + STS HA.
4. Security = Kerberos + Ranger/ACLs + TLS/at-rest encryption.
5. Design assuming any node dies; configure token renewal for long jobs.

## 🔗 Next Steps
- **Day 40**: Cost/Observability + Capstone Kickoff

## 📚 Additional Resources
- YARN Capacity/Fair Scheduler; Spark security & Kerberos docs; Ranger

---

**Progress**: Day 39/40 ✅
