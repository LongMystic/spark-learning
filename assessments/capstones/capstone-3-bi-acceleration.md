# Capstone 3: BI Acceleration

**Goal**: Make a slow "dashboard" query fast and cheap by designing a pre-aggregated
mart and the right join/serving strategy — without overloading the shared cluster.

## Scenario
A Superset dashboard runs several tiles like "total sales by category and region over
the last 30 days," currently querying the raw `transactions` fact (joined to
`stores`/`products`) directly through the Thrift Server.

## Steps
1. **Baseline**: implement the dashboard query against the **raw fact** (join +
   aggregate + date filter). Record time, bytes read, and join strategy from the SQL tab.
2. **Design the mart**: aggregate to the dashboard grain (`txn_date × category × region`),
   partition by `txn_date`, broadcast the small dimensions. Keep it small enough to cache.
3. **Serve from the mart**: rewrite the dashboard query against the mart. Compare
   time and bytes read to the baseline.
4. **Protect the shared engine**: describe (or configure) row limits, query timeout,
   fair-scheduler pool for BI, and a cache TTL aligned to the mart's refresh schedule.
5. **Refresh**: show how the mart would be rebuilt incrementally (Day 36 dbt-style
   or Day 35 Airflow job) so it stays fresh without full recomputation.

## Rubric
- [ ] Baseline vs mart measured (time + bytes read + join type).
- [ ] Mart is partitioned, compacted, and dimension joins are broadcast.
- [ ] Order-of-magnitude reduction in query cost demonstrated (at medium/large scale).
- [ ] A concrete plan to isolate BI load on the Thrift Server.
- [ ] Incremental refresh strategy described.

## Stretch
- Add DPP: partition the mart so a region/date filter prunes partitions, and show
  `dynamicpruning` firing.
