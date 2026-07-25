"""
Exercise 39: Shared-cluster architecture & multi-tenancy (design exercise)
Purpose: Reason about queue capacities, isolation, HA, and security. This is a
paper/design exercise — it prints a worked example and the questions to answer.

Run:  python exercises/production/exercise-39-architecture.py
"""

print("=" * 60)
print("Scenario: one on-prem Kubernetes cluster, 100 CPU / 400 GB allocatable, shared by:")
print("  - Team A (critical ETL)   - Team B (ad-hoc analytics)")
print("  - BI (Superset via STS)   - a streaming job (Kafka -> Iceberg)")
print("=" * 60)

print("""
Example namespace ResourceQuota design (requests / limits, ~guaranteed / burst):
  cluster (100 CPU / 400 GB allocatable)
   ├── ns: etl        requests 50 / limits 80    (Team A batch; priority)
   ├── ns: analytics  requests 20 / limits 50    (Team B ad-hoc; capped so it can't starve ETL)
   ├── ns: bi         requests 15 / limits 30    (Thrift Server; fair pools inside for users)
   └── ns: streaming  requests 15 / limits 15    (fixed: streaming needs steady, predictable slots)
  (numbers are CPU; give each namespace a matching requests/limits.memory too)

Isolation levers:
  - One namespace per tenant + ResourceQuota (above) + LimitRange (per-pod cap) + RBAC.
  - Dynamic allocation + shuffle tracking so idle executor pods return CPU/mem to the quota
    (Kubernetes has NO external shuffle service).
  - Fair-scheduler pools INSIDE the Thrift Server to isolate BI users.
  - Separate STS instances for BI vs heavy analytics if needed.

HA / recovery per component:
  - Driver (batch)   : idempotent + retryable job (Airflow retries).
  - Executor loss    : task retry + shuffle tracking / decommission block migration (no external shuffle service).
  - Control plane    : 3 control-plane nodes, etcd quorum (+ distributed MinIO / erasure coding for storage).
  - Thrift Server    : driver pod under a Deployment/StatefulSet + Service; load-balanced pair.
  - Streaming        : checkpoint -> exactly-once recovery.
  - Kafka            : replication factor >= 3.

Security:
  - AuthN: Kubernetes ServiceAccounts + RBAC; S3/MinIO keys in a Secret mounted into pods (no Kerberos keytab).
  - AuthZ: Ranger / OPA-Gatekeeper / storage ACLs (table/column/row).
  - Encryption: TLS in transit (spark.ssl.*), MinIO/S3 server-side encryption (SSE) at rest.
""")

print("Analysis Questions")
print("1. Why give streaming a FIXED (requests==limits) capacity?")
print("2. Which mechanism stops Team B's ad-hoc query from starving Team A's ETL?")
print("3. Why do long-running streaming/STS driver pods need secret/token rotation instead of Kerberos renewal?")
print("4. For each component, what is its HA/recovery mechanism?")
