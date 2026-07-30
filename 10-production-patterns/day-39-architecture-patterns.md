# Day 39: Architecture, Multi-Tenancy, HA & Security

## 🎯 Learning Objectives
- Choose batch/streaming architectures (lambda vs kappa) sensibly
- Share a cluster fairly across teams (namespaces + ResourceQuota, in-app scheduler pools)
- Design for high availability and graceful failure across every component
- Apply on-prem security basics (RBAC/ServiceAccounts, authorization, encryption)
- Reason about failure modes end-to-end, from a lost pod to a lost node

## 📚 Core Concepts

### 1. Batch, streaming, or both

| Pattern | Idea | Cost |
|---------|------|------|
| **Batch-only** | scheduled jobs (Airflow) | simplest; latency = schedule |
| **Lambda** | batch + speed layer, merged at query | powerful but **two codebases** to keep in sync |
| **Kappa** | one streaming pipeline; reprocess by replaying the log | one codebase; needs a durable log (Kafka) |

Most on-prem shops are batch-first and add streaming (Days 30-31) only where latency truly matters. Don't adopt lambda's complexity without the requirement.

**Key Points:**
- **Lambda architecture** runs the same logic twice — once as a batch job for the "correct, complete" view and once as a streaming job for a "fast, approximate" view — then reconciles them at query time. The cost isn't the infrastructure; it's maintaining **business logic in two places** that must stay semantically identical.
- **Kappa architecture** treats batch as a special case of streaming: a durable, replayable log (Kafka) is the single source of truth, and "reprocessing history" just means replaying the log from an earlier offset through the same streaming job. This needs a genuinely durable, replayable source — retrofitting Kappa onto a source system that only exposes "current state" (like a nightly database dump) doesn't work.
- **Decision point**: if a stakeholder can tell you a real latency requirement (e.g. "fraud alerts within 30 seconds") that batch genuinely cannot meet, streaming/Kappa is justified. If the answer is "it'd be nice to have it faster," a well-tuned batch schedule (Day 35) is almost always the lower-risk, lower-maintenance choice.

**Example:**
```
Batch-only:   Airflow (hourly/daily) -> Spark batch job -> Iceberg gold table -> BI
Kappa:        Kafka (durable log) -> Structured Streaming (Days 30-31) -> Iceberg (MERGE) -> BI
                    ^-- reprocessing = replay Kafka from an earlier offset through the SAME job
```

### 2. Multi-tenancy on Kubernetes

Shared clusters need **isolation** so one team can't starve another:
- **Namespace per team + `ResourceQuota` + `LimitRange`** with a guaranteed `requests` share and a `limits` burst ceiling (the analog of a scheduler queue's guaranteed/max capacity).
- **RBAC + ServiceAccounts** so each tenant can only act in its own namespace.
- **Spark fair-scheduler pools** within an app (e.g. the Thrift Server, Day 29).
- **Dynamic allocation + shuffle tracking** (no external shuffle service on K8S) so idle executor pods return their resources to the namespace quota.
- **Per-namespace quota caps** on total cores/memory (and `pods`) to cap blast radius.

**Key Points:**
- `ResourceQuota` sets hard ceilings on aggregate `requests`/`limits` (CPU, memory, and object counts like `pods`) **per namespace** — it's the Kubernetes-native equivalent of a YARN queue's min/max capacity.
- `LimitRange` complements it by bounding **individual** pod/container requests within a namespace (e.g. "no single executor pod may request more than 8 cores"), preventing one oversized job from consuming an entire namespace's quota by itself.
- RBAC `Role`/`RoleBinding` (namespace-scoped) restricts what a tenant's `ServiceAccount` can do — typically create/list/delete pods and the `SparkApplication` CRD in its own namespace only, never cluster-wide.

**Example:**
```yaml
apiVersion: v1
kind: ResourceQuota
metadata: { name: etl-quota, namespace: etl }
spec:
  hard:
    requests.cpu: "50"
    requests.memory: 200Gi
    limits.cpu: "80"
    limits.memory: 320Gi
    pods: "200"
---
apiVersion: v1
kind: LimitRange
metadata: { name: etl-limits, namespace: etl }
spec:
  limits:
    - type: Container
      default: { cpu: "2", memory: 8Gi }        # applied if a pod spec omits it
      max: { cpu: "8", memory: 32Gi }            # no single executor pod exceeds this
```

### 3. High availability

- **Kubernetes control-plane HA** (≥3 control-plane nodes, **etcd quorum**) and **distributed MinIO** (erasure coding) as the storage-HA answer instead of NameNode HA — so the platform survives a node loss.
- **Checkpointing** for streaming (Days 30-31) → recover exactly-once.
- **Idempotent, retryable jobs** (Days 21, 35, 38) → automatic recovery from transient failure.
- **Thrift Server HA** → run the driver pod under a Deployment/StatefulSet + a Service, load-balanced (Day 29).

**Key Points:**
- Control-plane HA and storage HA solve **different** failure classes: losing a control-plane node shouldn't stop already-running pods from working, while losing a MinIO node (with erasure coding) shouldn't lose or block access to data. Both need to be true independently.
- HA is layered: infrastructure HA (etcd/MinIO) keeps the *platform* up; application-level idempotency (checkpoints, MERGE, retries) keeps *individual jobs* correct when a pod or node underneath them dies mid-run. Neither substitutes for the other — infrastructure HA doesn't make a non-idempotent job safe to retry, and idempotent jobs still need a platform that can reschedule their pods.
- Executor pod loss during a shuffle-heavy stage is expected, routine behavior on Kubernetes, not an incident — Spark retries the lost tasks and (with shuffle tracking, Day 18/29) can recompute or migrate blocks rather than losing the whole job.

### 4. Priority and preemption on a shared queue

**Key Points:**
- `ResourceQuota` caps a namespace's *total* footprint, but says nothing about *ordering* when several tenants compete for the same free capacity at once — Kubernetes `PriorityClass` fills that gap, letting critical ETL pods preempt lower-priority ad-hoc pods when the cluster is genuinely full.
- Give the "critical batch ETL" namespace pods a higher `PriorityClass` than "ad-hoc analytics," mirroring the guaranteed/burst split from the `ResourceQuota` design: quota controls the ceiling, priority controls who wins a genuine capacity crunch inside it.
- Preemption is a blunt instrument — a preempted pod's work is lost (unless it was already checkpointing/idempotent, tying back to Day 35/38) — so reserve high priority for the workloads whose SLA truly outranks everything else, not as a default for every team that asks.

**Example:**
```yaml
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: { name: critical-etl }
value: 1000000
globalDefault: false
description: "Reserved for SLA-bound nightly ETL; can preempt ad-hoc analytics pods."
---
apiVersion: scheduling.k8s.io/v1
kind: PriorityClass
metadata: { name: adhoc-analytics }
value: 100
globalDefault: true
description: "Default for exploratory/ad-hoc workloads; preemptible."
```

## 🔍 Deep Dive: Security on-prem

### Step-by-Step Process

1. **Authentication**: Kerberos is replaced by Kubernetes **ServiceAccounts + RBAC**. Pods authenticate to the API server with their ServiceAccount token; S3/MinIO access keys live in a **Secret** mounted into the driver/executor pods (`envFrom: secretRef`) — no keytabs to distribute.
2. **Authorization**: Ranger (or **OPA/Gatekeeper** for admission policy) and storage ACLs control table/column/row access; Iceberg/Hive integrate with these. Kubernetes **RBAC + ResourceQuota** bound what each tenant can create.
3. **Encryption**: in transit (RPC/shuffle/UI TLS: `spark.ssl.*`, `spark.network.crypto.enabled`) and at rest via **S3/MinIO server-side encryption (SSE)**.
4. **Isolation**: run per-tenant in separate **namespaces** with their own ServiceAccount + Secret; avoid sharing one credential across teams.

### Example: A tenant's security surface, end to end

```yaml
# 1. Namespace-scoped identity: this ServiceAccount can only act inside ns "etl"
apiVersion: v1
kind: ServiceAccount
metadata: { name: spark-etl, namespace: etl }
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata: { name: spark-driver-role, namespace: etl }
rules:
  - apiGroups: [""]
    resources: ["pods", "services", "configmaps"]
    verbs: ["create", "get", "list", "watch", "delete"]
  - apiGroups: ["sparkoperator.k8s.io"]
    resources: ["sparkapplications"]
    verbs: ["create", "get", "list", "watch", "update", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata: { name: spark-driver-binding, namespace: etl }
subjects: [{ kind: ServiceAccount, name: spark-etl, namespace: etl }]
roleRef: { kind: Role, name: spark-driver-role, apiGroup: rbac.authorization.k8s.io }
---
# 2. Credentials as a mounted Secret, never hardcoded in the SparkApplication spec
apiVersion: v1
kind: Secret
metadata: { name: minio-creds, namespace: etl }
type: Opaque
stringData:
  AWS_ACCESS_KEY_ID: "..."
  AWS_SECRET_ACCESS_KEY: "..."
```
```yaml
# 3. The SparkApplication references both: identity via serviceAccount, secrets via envFrom
spec:
  driver:
    serviceAccount: spark-etl
    envFrom: [{ secretRef: { name: minio-creds } }]
  executor:
    envFrom: [{ secretRef: { name: minio-creds } }]
  sparkConf:
    "spark.ssl.enabled": "true"
    "spark.network.crypto.enabled": "true"
```

**Analysis:**
- Every layer of this example is namespace-scoped: the `ServiceAccount`, the `Role`/`RoleBinding`, and the `Secret` all live in `etl` and cannot reach across into another tenant's namespace — this is what makes "namespace per team" an actual security boundary, not just an organizational label.
- Because credentials live in a `Secret` referenced by name (not embedded in the CRD/YAML committed to git), rotating the MinIO access key is a one-namespace operation (`kubectl apply` a new `Secret`, restart the affected pods) instead of a fleet-wide credential hunt.
- `spark.network.crypto.enabled` protects shuffle data in flight between executor pods — relevant on a shared cluster where network traffic between pods may traverse the same physical switches as other tenants' traffic.

## 💡 Key Insights for On-Premise

### 1. Right-size at the platform, not per job
On a shared cluster, a job asking for 500 executors just moves a bottleneck. Set sane per-namespace `ResourceQuota` caps and let dynamic allocation flex within them; coordinate big jobs with the platform team.

### 2. Design for the node that will die
Assume any executor pod/node can vanish. Everything you learned — shuffle tracking (or decommissioning with block migration), checkpoints, idempotent writes, retries — exists so a single failure is a hiccup, not an incident.

### 3. Credential/secret rotation for long-running apps
Kubernetes removes the Kerberos token-expiry gotcha, but long-lived Streaming/Thrift driver pods still depend on the **Secret** holding S3/MinIO keys (and their ServiceAccount token). Rotate secrets deliberately and restart or re-mount so a days-long app doesn't fail on a stale credential — the K8S analog of the old keytab-renewal problem.

### 4. Namespace quotas are a negotiation, not a one-time config
As teams onboard new pipelines, per-namespace `ResourceQuota` caps need periodic review against actual usage (Day 40's cost/observability metrics feed directly into this). Treat quota changes as a planned, visible process — silent quota bumps are how one team's "just this once" becomes permanent cluster-wide contention.

### 5. Admission policy (OPA/Gatekeeper) catches what RBAC can't
RBAC controls *who* can create a resource; admission policies control *what* that resource is allowed to look like — e.g. rejecting a `SparkApplication` that requests more cores than the `LimitRange` allows, or that omits a required `serviceAccount`. Use both layers together rather than relying on RBAC alone to keep tenants well-behaved.

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

### Exercise 3: Write the RBAC + Secret manifests for a new tenant
```python
# Extend exercise 39: for a new "marketing-analytics" namespace, write out (as YAML in
# comments or a string) the ServiceAccount, Role, RoleBinding, ResourceQuota, and LimitRange
# it needs, following the least-privilege pattern from the Deep Dive. State explicitly what
# this tenant CANNOT do that a cluster-admin could.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Per-namespace quota utilization & pending pods** (starvation) — a namespace consistently near 100% of its `ResourceQuota` with pods stuck `Pending` is a capacity-planning signal, not just a today problem.
2. **Job success rate across tenants** — compare tenants; one namespace with a much lower success rate may indicate misconfigured retries or a `LimitRange` that's too tight for its workload.
3. **Secret/token-rotation and RBAC auth failures** on long-running apps — a spike in `403`/auth errors from a Thrift Server or streaming driver pod often means a rotated Secret wasn't re-mounted.
4. **etcd/control-plane health** (leader elections, latency) — control-plane instability affects every tenant simultaneously, so it deserves cluster-wide alerting independent of any single namespace.
5. **Admission-policy rejection rate** (OPA/Gatekeeper) — frequent rejections may mean tenants need clearer self-service templates, not just a stricter gate.

### Spark UI Analysis
- For a Thrift Server or streaming driver running under a Deployment, confirm in `kubectl get pods` (and the Spark UI's Executors tab once a replacement pod is up) that a restarted driver pod picks back up with the expected executor count — this is the practical verification that "HA" actually works, not just that the manifest says `replicas: 1` with a restart policy.
- After a deliberate executor-pod kill (chaos-test style), check the **Stages** tab for the affected stage: tasks should show as retried/succeeded on a different executor, and total stage time should reflect one extra task attempt, not a full job restart.

## 🚨 Common Issues & Solutions

### Issue 1: One team starves the cluster
**Symptom**: others' pods stay `Pending` forever.
**Root Cause**: no per-namespace `ResourceQuota`, or a quota set so generously that one team's jobs can consume nearly all cluster capacity.
**Solution**: namespace `ResourceQuota` + max-executor caps + fair pools.

### Issue 2: Long-running app dies after N hours
**Symptom**: auth/credential failure mid-run.
**Root Cause**: the S3/MinIO `Secret` (or the pod's ServiceAccount token) was rotated without restarting or re-mounting it into the long-lived driver pod.
**Solution**: rotate and re-mount the S3/MinIO **Secret** (and ServiceAccount token) so long-lived driver pods keep valid credentials; automate the restart as part of the rotation process.

### Issue 3: A single oversized job blows through the namespace quota
**Symptom**: one job's executor pods alone consume the entire namespace `ResourceQuota`, blocking every other job in that namespace.
**Root Cause**: no `LimitRange` (or an overly generous one) bounding individual pod requests, so nothing stops one `SparkApplication` from requesting an outsized share.
**Solution**: set a `LimitRange` max per container/pod alongside the namespace `ResourceQuota`, and cap `spark.dynamicAllocation.maxExecutors` per job as a second guardrail.

### Issue 4: Admission policy silently blocks a legitimate job
**Symptom**: a `SparkApplication` is rejected at creation with an opaque webhook error.
**Root Cause**: an OPA/Gatekeeper policy (e.g. requiring a `serviceAccount` field, or capping `driver.cores`) rejects the manifest, but the error surfaced to the submitting pipeline is unclear.
**Solution**: keep admission-policy messages human-readable and documented, and give teams a validated template (Day 35's `SparkApplication` YAML) that already satisfies the policy so they rarely hit the gate directly.

### Issue 5: Streaming job can't survive a control-plane hiccup
**Symptom**: a long-running Structured Streaming job's driver pod disappears during a control-plane blip and doesn't resume correctly.
**Root Cause**: the driver was run under a bare `Pod` instead of a `Deployment`/`SparkApplication` with `restartPolicy`, so nothing rescheduled it, or checkpointing (Day 30-31) wasn't configured, so even a successful restart couldn't resume from the last committed offset.
**Solution**: run streaming drivers under a controller that restarts them automatically, and always configure checkpointing so a restart resumes exactly where it left off.

### Issue 6: Ad-hoc analytics pods keep getting preempted mid-query
**Symptom**: exploratory Spark jobs in the analytics namespace are killed and rescheduled repeatedly during business hours.
**Root Cause**: `PriorityClass` was applied more aggressively than intended — critical-ETL priority is set so high, and its `ResourceQuota` so loose, that it preempts analytics workloads even when the cluster isn't genuinely capacity-constrained, rather than only during real contention.
**Solution**: keep `ResourceQuota` as the primary capacity guardrail and use `PriorityClass`/preemption only as a tie-breaker for genuine contention; review preemption events (`kubectl get events --field-selector reason=Preempted`) to confirm they correlate with real capacity crunches, not routine scheduling noise.

## 📝 Key Takeaways
1. Prefer batch-first; add streaming/lambda/kappa only when a real latency requirement justifies the added complexity.
2. Isolate tenants with namespaces + ResourceQuota + LimitRange + RBAC + fair pools + dynamic allocation.
3. HA = control-plane/etcd quorum + distributed MinIO + checkpoints + idempotent retryable jobs + STS HA — infrastructure HA and application idempotency are complementary, not substitutes.
4. Security = ServiceAccounts/Secrets + RBAC + Ranger/OPA + TLS/SSE-at-rest, all scoped per namespace.
5. Design assuming any node dies; rotate secrets/tokens for long jobs, and restart to pick up the rotation.
6. Review namespace quotas periodically against real usage rather than treating them as a one-time setup step.
7. Admission policies (OPA/Gatekeeper) catch misconfigurations RBAC alone can't.
8. `PriorityClass`/preemption is a tie-breaker for genuine contention, layered on top of `ResourceQuota` — not a substitute for it.

## 🔗 Next Steps
- **Day 40**: Cost/Observability + Capstone Kickoff

## 📚 Additional Resources
- Kubernetes RBAC, ResourceQuota & LimitRange docs
- Spark-on-Kubernetes security docs (`spark.ssl.*`, `spark.network.crypto.enabled`)
- Ranger / OPA-Gatekeeper admission policy docs
- Lambda vs Kappa architecture references

---

**Progress**: Day 39/40 ✅
