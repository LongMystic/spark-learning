# Day 39: Architecture, Multi-Tenancy, HA & Security

## 🎯 Learning Objectives
- Choose batch/streaming architectures (lambda vs kappa) sensibly
- Share a cluster fairly across teams (namespaces + ResourceQuota, in-app scheduler pools)
- Design for high availability and graceful failure
- Apply on-prem security basics (RBAC/ServiceAccounts, authorization, encryption)

## 📚 Core Concepts

### 1. Batch, streaming, or both
| Pattern | Idea | Cost |
|---------|------|------|
| **Batch-only** | scheduled jobs (Airflow) | simplest; latency = schedule |
| **Lambda** | batch + speed layer, merged at query | powerful but **two codebases** to keep in sync |
| **Kappa** | one streaming pipeline; reprocess by replaying the log | one codebase; needs a durable log (Kafka) |
Most on-prem shops are batch-first and add streaming (Days 30–31) only where latency truly matters. Don't adopt lambda's complexity without the requirement.

### 2. Multi-tenancy on Kubernetes
Shared clusters need **isolation** so one team can't starve another:
- **Namespace per team + `ResourceQuota` + `LimitRange`** with a guaranteed `requests` share and a `limits` burst ceiling (the analog of a scheduler queue's guaranteed/max capacity).
- **RBAC + ServiceAccounts** so each tenant can only act in its own namespace.
- **Spark fair-scheduler pools** within an app (e.g. the Thrift Server, Day 29).
- **Dynamic allocation + shuffle tracking** (no external shuffle service on K8S) so idle executor pods return their resources to the namespace quota.
- **Per-namespace quota caps** on total cores/memory (and `pods`) to cap blast radius.

### 3. High availability
- **Kubernetes control-plane HA** (≥3 control-plane nodes, **etcd quorum**) and **distributed MinIO** (erasure coding) as the storage-HA answer instead of NameNode HA — so the platform survives a node loss.
- **Checkpointing** for streaming (Days 30–31) → recover exactly-once.
- **Idempotent, retryable jobs** (Days 21, 35, 38) → automatic recovery from transient failure.
- **Thrift Server HA** → run the driver pod under a Deployment/StatefulSet + a Service, load-balanced (Day 29).

## 🔍 Deep Dive: Security on-prem
- **Authentication**: Kerberos is replaced by Kubernetes **ServiceAccounts + RBAC**. Pods authenticate to the API server with their ServiceAccount token; S3/MinIO access keys live in a **Secret** mounted into the driver/executor pods (`envFrom: secretRef`) — no keytabs to distribute.
- **Authorization**: Ranger (or **OPA/Gatekeeper** for admission policy) and storage ACLs control table/column/row access; Iceberg/Hive integrate with these. K8S **RBAC + ResourceQuota** bound what each tenant can create.
- **Encryption**: in transit (RPC/shuffle/UI TLS: `spark.ssl.*`, `spark.network.crypto.enabled`) and at rest via **S3/MinIO server-side encryption (SSE)**.
- **Isolation**: run per-tenant in separate **namespaces** with their own ServiceAccount + Secret; avoid sharing one credential across teams.

## 💡 Key Insights for On-Premise
### 1. Right-size at the platform, not per job
On a shared cluster, a job asking for 500 executors just moves a bottleneck. Set sane per-namespace `ResourceQuota` caps and let dynamic allocation flex within them; coordinate big jobs with the platform team.

### 2. Design for the node that will die
Assume any executor pod/node can vanish. Everything you learned — shuffle tracking (or decommissioning with block migration), checkpoints, idempotent writes, retries — exists so a single failure is a hiccup, not an incident.

### 3. Credential/secret rotation for long-running apps
Kubernetes removes the Kerberos token-expiry gotcha, but long-lived Streaming/Thrift driver pods still depend on the **Secret** holding S3/MinIO keys (and their ServiceAccount token). Rotate secrets deliberately and restart or re-mount so a days-long app doesn't fail on a stale credential — the K8S analog of the old keytab-renewal problem.

## 🎯 Practical Exercises

### Exercise 1: Reason about a shared-cluster design
```python
# See exercises/production/exercise-39-architecture.py
# Given 3 teams + BI + streaming, propose namespace ResourceQuotas and isolation; print the config.
```

### Exercise 2: Failure-mode checklist
```python
# For each component (driver pod, executor pod, control plane, STS, Kafka), state the HA/recovery mechanism.
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Per-namespace quota utilization & pending pods (starvation).
2. Job success rate across tenants.
3. Secret/token-rotation and RBAC auth failures on long-running apps.

## 🚨 Common Issues & Solutions

### Issue 1: One team starves the cluster
**Symptom**: others' pods stay `Pending` forever.
**Solution**: namespace `ResourceQuota` + max-executor caps + fair pools.

### Issue 2: Long-running app dies after N hours
**Symptom**: auth/credential failure mid-run.
**Solution**: rotate and re-mount the S3/MinIO **Secret** (and ServiceAccount token) so long-lived driver pods keep valid credentials.

## 📝 Key Takeaways
1. Prefer batch-first; add streaming/lambda/kappa only when justified.
2. Isolate tenants with namespaces + ResourceQuota + fair pools + dynamic allocation.
3. HA = control-plane/etcd quorum + distributed MinIO + checkpoints + idempotent retryable jobs + STS HA.
4. Security = ServiceAccounts/Secrets + RBAC + Ranger/OPA + TLS/SSE-at-rest.
5. Design assuming any node dies; rotate secrets/tokens for long jobs.

## 🔗 Next Steps
- **Day 40**: Cost/Observability + Capstone Kickoff

## 📚 Additional Resources
- Kubernetes RBAC, ResourceQuota & LimitRange; Spark-on-Kubernetes security docs; Ranger / OPA-Gatekeeper

---

**Progress**: Day 39/40 ✅
