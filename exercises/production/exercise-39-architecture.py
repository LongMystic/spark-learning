"""
Exercise 39: Shared-cluster architecture & multi-tenancy (design exercise)
Purpose: Reason about queue capacities, isolation, HA, and security. This is a
paper/design exercise — it prints a worked example and the questions to answer.

Run:  python exercises/production/exercise-39-architecture.py
"""

print("=" * 60)
print("Scenario: one on-prem YARN cluster, 100 vcores / 400 GB, shared by:")
print("  - Team A (critical ETL)   - Team B (ad-hoc analytics)")
print("  - BI (Superset via STS)   - a streaming job (Kafka -> Iceberg)")
print("=" * 60)

print("""
Example capacity-scheduler design (guaranteed / max):
  root
   ├── etl        50% / 80%   (Team A batch; priority)
   ├── analytics  20% / 50%   (Team B ad-hoc; capped so it can't starve ETL)
   ├── bi         15% / 30%   (Thrift Server; fair pools inside for users)
   └── streaming  15% / 15%   (fixed: streaming needs steady, predictable slots)

Isolation levers:
  - Capacity scheduler queues (above) + per-queue max-application/AM limits.
  - Dynamic allocation + external shuffle service so idle slots return to the pool.
  - Fair-scheduler pools INSIDE the Thrift Server to isolate BI users.
  - Separate STS instances for BI vs heavy analytics if needed.

HA / recovery per component:
  - Driver (batch)   : idempotent + retryable job (Airflow retries).
  - Executor loss    : task retry + external shuffle service (no lost shuffle files).
  - ResourceManager  : YARN RM HA (standby).
  - Thrift Server    : supervised + load-balanced pair.
  - Streaming        : checkpoint -> exactly-once recovery.
  - Kafka            : replication factor >= 3.

Security:
  - AuthN: Kerberos (keytab + principal; token renewal for long jobs).
  - AuthZ: Ranger / storage ACLs (table/column/row).
  - Encryption: TLS in transit (spark.ssl.*), HDFS encryption zones at rest.
""")

print("Analysis Questions")
print("1. Why give streaming a FIXED (guaranteed==max) capacity?")
print("2. Which mechanism stops Team B's ad-hoc query from starving Team A's ETL?")
print("3. Why do long-running streaming/STS jobs need Kerberos token renewal?")
print("4. For each component, what is its HA/recovery mechanism?")
