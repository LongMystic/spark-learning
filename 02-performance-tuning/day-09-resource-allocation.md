# Day 9: Resource Allocation and Kubernetes Integration

## 🎯 Learning Objectives
- Understand how Spark requests resources from Kubernetes
- Master dynamic allocation on K8S (shuffle tracking — there is no external shuffle service)
- Learn how pods, requests, and limits map to executors
- Optimize resource usage for on-premise Kubernetes clusters
- Handle resource contention with namespaces, quotas, and scheduling

## 📚 Core Concepts

### 1. Kubernetes Resource Model

**Kubernetes Components:**
- **API server + kube-scheduler**: the cluster-wide brain that places pods on nodes (the role YARN's ResourceManager played)
- **kubelet**: the per-node agent that runs containers and reports capacity (the role of YARN's NodeManager)
- **Driver pod**: in cluster mode the Spark driver runs as a pod and requests executor pods directly from the API server (the role of YARN's ApplicationMaster)
- **Pod**: the unit of resource allocation — one driver pod + N executor pods (the analog of a YARN container)

**Resource Types:**
- **Memory**: bytes/MiB/GiB, expressed as pod `requests` (reserved) and `limits` (hard cap)
- **CPU**: millicores/cores, e.g. `500m` or `5`

**Resource Allocation:**
```python
# Spark's driver pod calls the Kubernetes API
# The scheduler places executor pods on nodes with free capacity
# Each executor runs in its own pod, sized by requests/limits
```

### 2. Static vs Dynamic Allocation

**Static Allocation:**
```python
# Fixed number of executor pods
spark.conf.set("spark.executor.instances", "30")
spark.conf.set("spark.dynamicAllocation.enabled", "false")
```

**Dynamic Allocation:**
```python
# Executor pods created/removed based on demand.
# On K8S there is NO external shuffle service, so you MUST enable shuffle tracking
# to avoid throwing away shuffle data when an executor is removed.
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")
```

### 3. Kubernetes Configuration

**Key K8S-side settings (platform team owns these):**
- Node **allocatable** memory/CPU: node capacity minus kubelet/system reservations (`--kube-reserved`, `--system-reserved`) — the analog of `yarn.nodemanager.resource.memory-mb`
- **ResourceQuota** per namespace: caps a tenant's total requests/limits — the analog of a scheduler queue's capacity
- **LimitRange** per namespace: caps a single pod's size — the analog of `yarn.scheduler.maximum-allocation-*`

**Spark-Kubernetes Integration:**
```bash
# Submit to Kubernetes
spark-submit \
  --master k8s://https://<api-server>:6443 \
  --deploy-mode cluster \
  --conf spark.kubernetes.namespace=spark-jobs \
  --conf spark.kubernetes.container.image=<registry>/spark:3.5.1 \
  --conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
  --conf spark.executor.memory=14g \
  --conf spark.executor.cores=5 \
  --conf spark.executor.instances=30 \
  local:///opt/spark/work-dir/app.py
```

## 🔍 Deep Dive: Dynamic Allocation

### How Dynamic Allocation Works

**Allocation Process:**
1. **Initial**: Start with `initialExecutors`
2. **Scale Up**: Add executor pods when tasks are pending
3. **Scale Down**: Remove idle executor pods after timeout
4. **Bounds**: Respect `minExecutors` and `maxExecutors`

**Why shuffle tracking is mandatory on K8S:** On YARN, the NodeManager ran an *external
shuffle service* that kept serving an executor's shuffle files after the executor died,
so dynamic allocation could freely remove idle executors. **Kubernetes has no external
shuffle service.** Instead, `spark.dynamicAllocation.shuffleTracking.enabled=true` makes
Spark keep an executor alive as long as it still holds shuffle blocks a later stage needs.

**Configuration:**
```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")

# Executor bounds
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")

# Scaling behavior
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
spark.conf.set("spark.dynamicAllocation.shuffleTracking.timeout", "30m")  # keep shuffle-holders this long
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
spark.conf.set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "5s")
```

> Alternative to shuffle tracking: **executor decommissioning with block migration**
> (`spark.decommission.enabled=true`, `spark.storage.decommission.shuffleBlocks.enabled=true`)
> moves shuffle/cache blocks off an executor before it's removed. Or mount a PVC for shuffle
> data so it survives pod restarts.

### When to Use Dynamic Allocation

**Good For:**
- Variable workloads
- Multiple concurrent applications
- Resource sharing across namespaces
- Cost / node-utilization optimization (works well with the cluster autoscaler)

**Not Good For:**
- Predictable, consistent workloads
- Low-latency requirements
- When you need guaranteed resources

### Dynamic Allocation Behavior

**Scale Up Triggers:**
- Pending tasks in queue
- Backlog timeout exceeded
- Sustained backlog timeout exceeded

**Scale Down Triggers:**
- Executor idle for `executorIdleTimeout`
- No active shuffle blocks (shuffle tracking) and no cached data

**Example:**
```python
# Job starts with 10 executor pods
# Tasks queue up → scheduler creates pods up to 30
# Tasks complete → idle pods deleted down to 5 (min)
# Pods holding shuffle blocks are kept until shuffleTracking.timeout
```

## 💡 Resource Allocation Strategies

### 1. Conservative Allocation

**Strategy**: Request fewer resources, scale up if needed

**Configuration:**
```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "30")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")
```

**Use Case**: Shared cluster, multiple teams/namespaces

### 2. Aggressive Allocation

**Strategy**: Request more resources upfront

**Configuration:**
```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "20")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "100")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "50")
```

**Use Case**: Dedicated node pool, performance critical

### 3. Static Allocation

**Strategy**: Fixed resources, no scaling

**Configuration:**
```python
spark.conf.set("spark.dynamicAllocation.enabled", "false")
spark.conf.set("spark.executor.instances", "30")
```

**Use Case**: Predictable workloads, guaranteed resources

### 4. Hybrid Approach

**Strategy**: Static base, dynamic for peaks

**Configuration:**
```python
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "20")  # Base load
spark.conf.set("spark.dynamicAllocation.maxExecutors", "50")  # Peak load
```

## 🔍 Deep Dive: Kubernetes Integration

### Pod Sizing

**Executor pod size:**
```python
# Pod memory request == limit == executor memory + overhead.
executor_memory = 14 * 1024        # 14GB heap, in MB
executor_memoryOverhead = 2 * 1024 # 2GB off-heap + Python (PySpark!), in MB
pod_memory = executor_memory + executor_memoryOverhead  # 16GB pod memory limit

# Executor CPU
executor_cores = 5                 # task slots inside the pod
# spark.kubernetes.executor.request.cores = "5"  # what the scheduler reserves
# spark.kubernetes.executor.limit.cores   = "5"  # hard CPU cap (optional)
```

`spark.kubernetes.memoryOverheadFactor` (default 0.1) sets the overhead automatically when
you don't specify `spark.executor.memoryOverhead`. **Bump it for PySpark** — Python runs
outside the JVM heap.

**Kubernetes constraints:**
- Pod memory/CPU request must fit a node's **allocatable** capacity
- Pod size must be ≤ the namespace **LimitRange** max
- Namespace total ≤ its **ResourceQuota**

### Resource Negotiation

**Request Process:**
1. The driver pod asks the API server to create executor pods with given requests
2. The kube-scheduler finds nodes with enough free allocatable capacity
3. Pods are bound to nodes and the kubelet starts the containers
4. Executors register back with the Spark driver
5. If no node fits, pods stay **Pending** (analogous to a YARN container waiting on capacity)

**Common Issues:**
- **Insufficient Resources**: no node has room → pods stuck `Pending` (and the cluster autoscaler, if present, adds a node)
- **Quota Exhausted**: namespace `ResourceQuota` reached → pod creation rejected
- **Pod too large**: request exceeds `LimitRange` max or any single node's allocatable

### Namespace & Quota Configuration

**Namespaces replace YARN queues.** Submit into a namespace:
```bash
spark-submit \
  --master k8s://https://<api-server>:6443 \
  --conf spark.kubernetes.namespace=production \
  app.py

# Or in code
spark.conf.set("spark.kubernetes.namespace", "production")
```

**ResourceQuota properties (per namespace):**
- **requests.cpu / requests.memory**: guaranteed share (≈ queue capacity)
- **limits.cpu / limits.memory**: burst ceiling (≈ queue max-capacity)
- **pods**: cap on concurrent pods (per-tenant limit)

## 💡 Key Insights for On-Premise

### 1. Node & Namespace Configuration

**Node reservations (kubelet flags, platform team):**
```
# On a 64GB / 16-core node, leave headroom for the OS + kubelet:
--system-reserved=cpu=1,memory=2Gi
--kube-reserved=cpu=1,memory=2Gi
# → ~48-60GB and ~14 cores become *allocatable* to pods
```

**Per-pod cap (LimitRange, analog of yarn.scheduler.maximum-allocation-*):**
```yaml
apiVersion: v1
kind: LimitRange
metadata:
  name: pod-limits
  namespace: spark-jobs
spec:
  limits:
    - type: Container
      max: { cpu: "8", memory: 16Gi }   # biggest executor pod allowed
```

### 2. Dynamic Allocation Tuning

**For Interactive Workloads:**
```python
# Keep executors longer for cached data
spark.conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "infinity")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "300s")
```

**For Batch Workloads:**
```python
# Aggressive scaling
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "60s")
```

**For Streaming:**
```python
# Static allocation recommended
spark.conf.set("spark.dynamicAllocation.enabled", "false")
spark.conf.set("spark.executor.instances", "20")
```

### 3. Multi-Tenancy (Namespaces + Quotas)

**Create a namespace per tenant class:**
- Production workloads (guaranteed quota)
- Development workloads (smaller quota)
- ETL pipelines (scheduled)
- Ad-hoc queries (best-effort / low quota)

**Namespace + Quota configuration:**
```yaml
apiVersion: v1
kind: Namespace
metadata: { name: production }
---
apiVersion: v1
kind: ResourceQuota
metadata: { name: production-quota, namespace: production }
spec:
  hard:
    requests.cpu: "200"        # guaranteed capacity
    requests.memory: 400Gi
    limits.cpu: "320"          # burst ceiling (max-capacity)
    limits.memory: 640Gi
---
apiVersion: v1
kind: ResourceQuota
metadata: { name: development-quota, namespace: development }
spec:
  hard:
    requests.cpu: "120"
    requests.memory: 240Gi
```

## 🎯 Practical Exercises

### Exercise 1: Configure Dynamic Allocation

```python
# 1. Enable dynamic allocation (+ shuffle tracking, required on K8S)
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")

# 2. Set bounds
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "30")
spark.conf.set("spark.dynamicAllocation.initialExecutors", "10")

# 3. Run a workload
df = spark.read.parquet("s3a://warehouse/large_table/")
result = df.groupBy("key").agg(sum("amount"))

# 4. Monitor:
#    - Spark UI Executors tab: Watch executor count change
#    - kubectl -n spark-jobs get pods -w : watch executor pods appear/disappear
```

### Exercise 2: Compare Static vs Dynamic

```python
# Configuration 1: Static
spark.conf.set("spark.dynamicAllocation.enabled", "false")
spark.conf.set("spark.executor.instances", "20")
# Run query and measure time

# Configuration 2: Dynamic
spark.conf.set("spark.dynamicAllocation.enabled", "true")
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
spark.conf.set("spark.dynamicAllocation.minExecutors", "5")
spark.conf.set("spark.dynamicAllocation.maxExecutors", "30")
# Run same query and measure time

# Compare:
# - Execution time
# - Node/resource utilization
# - Cost (if applicable)
```

### Exercise 3: Monitor Resource Allocation

```python
# 1. Run job with dynamic allocation
# 2. Watch Kubernetes:
#    kubectl -n spark-jobs get pods -w      # pods created/deleted
#    kubectl top pods -n spark-jobs         # live CPU/mem per pod
#    kubectl describe pod <executor-pod>    # requests/limits, events, scheduling
# 3. Check Spark UI (port-forward the driver :4040):
#    - Executor count over time
#    - Resource usage per executor
# 4. Analyze:
#    - When executor pods were added/removed
#    - Resource utilization patterns
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor

1. **Pod scheduling latency / count of `Pending` pods**: Signals insufficient allocatable capacity or quota exhaustion
2. **Namespace quota utilization**: `requests`/`limits` consumed vs the namespace `ResourceQuota`
3. **Node allocatable vs used** (`kubectl top nodes`): Headroom left for new executor pods
4. **Executor lifecycle**: Pod add/remove events as dynamic allocation scales up and down

### Spark UI Analysis

- **Executors tab** (port-forward the driver `:4040`): Watch executor count rise and fall as dynamic allocation reacts to load, and check per-executor memory/GC
- **Combine with kubectl**: `kubectl -n spark-jobs get pods -w` (pod lifecycle), `kubectl top pods` (live CPU/mem), `kubectl describe pod <pod>` (scheduling events) alongside the Spark UI for the full picture
- **Cluster monitoring**: Prometheus + Grafana (via Spark's Prometheus servlet) and the Kubernetes Dashboard for longer-term trends across jobs

## 🚨 Common Issues & Solutions

### Issue 1: Executor Pods Not Scheduled

**Symptom**: Job stuck, executor pods stay `Pending`

**Root Causes:**
- Insufficient node allocatable capacity
- Namespace `ResourceQuota` exhausted
- Pod request exceeds `LimitRange` max or any node's capacity

**Solution:**
```python
# Shrink the pod so it fits available nodes
spark.conf.set("spark.executor.memory", "8g")  # Instead of 16g
spark.conf.set("spark.executor.cores", "4")     # Instead of 8

# Or submit into a namespace with free quota
spark.conf.set("spark.kubernetes.namespace", "development")
```
```bash
kubectl -n spark-jobs describe pod <pending-pod>   # Events explain WHY it's pending
```

### Issue 2: Slow Executor Startup

**Symptom**: Long wait before executors register

**Root Cause**: Image pulls, node contention, autoscaler adding nodes

**Solution:**
```python
# Start with more executors so the app isn't starved early
spark.conf.set("spark.dynamicAllocation.initialExecutors", "20")

# Or use static allocation for critical jobs
spark.conf.set("spark.dynamicAllocation.enabled", "false")
spark.conf.set("spark.executor.instances", "30")
```
```
# Pre-pull the Spark image onto nodes (imagePullPolicy: IfNotPresent) to cut startup.
```

### Issue 3: Executors Removed Too Quickly

**Symptom**: Executor pods removed before the next stage

**Root Cause**: Idle timeout too short, or shuffle tracking off

**Solution:**
```python
# Increase idle timeout
spark.conf.set("spark.dynamicAllocation.executorIdleTimeout", "300s")

# Keep executors holding shuffle blocks (K8S has no external shuffle service!)
spark.conf.set("spark.dynamicAllocation.shuffleTracking.enabled", "true")
spark.conf.set("spark.dynamicAllocation.shuffleTracking.timeout", "30m")

# Keep executors with cached data
spark.conf.set("spark.dynamicAllocation.cachedExecutorIdleTimeout", "infinity")
```

### Issue 4: Not Scaling Up Enough

**Symptom**: Slow performance, pending tasks

**Root Cause**: Max executors too low, quota cap, or scaling too slow

**Solution:**
```python
# Increase max executors
spark.conf.set("spark.dynamicAllocation.maxExecutors", "100")

# Faster scaling
spark.conf.set("spark.dynamicAllocation.schedulerBacklogTimeout", "1s")
spark.conf.set("spark.dynamicAllocation.sustainedSchedulerBacklogTimeout", "3s")
# Also check the namespace ResourceQuota isn't the ceiling.
```

### Issue 5: Pods Pending Despite Free Capacity

**Symptom**: Cluster shows free CPU/RAM but pods won't schedule

**Root Cause**: Fragmentation — no *single* node has room for the requested pod size; or affinity/taint rules exclude nodes

**Solution:**
```python
# Reduce pod size so it fits a node
spark.conf.set("spark.executor.memory", "8g")
spark.conf.set("spark.executor.cores", "4")

# Or request fewer initial executors
spark.conf.set("spark.dynamicAllocation.initialExecutors", "5")
```
```
# Review node affinity / taints in the pod template if pods avoid otherwise-free nodes.
```

## 📝 Key Takeaways

1. **Dynamic allocation** adapts to workload demand — enable **shuffle tracking** on K8S
2. **Kubernetes schedules pods**; the driver pod requests executor pods directly
3. **Pod request/limit** = executor memory + overhead; must fit node allocatable
4. **Namespaces + ResourceQuota** replace YARN queues for multi-tenancy
5. **Monitor pods** (`kubectl get/top/describe`) to optimize performance
6. **Static allocation** for predictable workloads
7. **Dynamic allocation** for variable workloads
8. **Right-size pods** so they fit a node and the LimitRange

## 🔗 Next Steps

- **Day 10**: Data Skew Handling
- Practice: Configure dynamic allocation (with shuffle tracking) for your workloads
- Experiment: Compare static vs dynamic allocation, watch `kubectl get pods -w`
- Monitor: Track pod scheduling and namespace quota utilization

## 📚 Additional Resources

- [Dynamic Allocation Guide](https://spark.apache.org/docs/latest/job-scheduling.html#dynamic-resource-allocation)
- [Running Spark on Kubernetes](https://spark.apache.org/docs/latest/running-on-kubernetes.html)
- [Kubernetes ResourceQuota](https://kubernetes.io/docs/concepts/policy/resource-quotas/)

---

**Progress**: Day 9/40 ✅
