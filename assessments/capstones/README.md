# Capstone Projects

End-to-end projects that exercise the whole path on the local sample data (or your
real cluster). Each has a **goal**, **starter steps**, and a **rubric**. Pick at
least one; do all three for true mastery.

| # | Capstone | Exercises the skills of |
|---|----------|-------------------------|
| 1 | [Skew Hunt & Fix](capstone-1-skew-hunt.md) | Phases 1-3 (execution, tuning, debugging) |
| 2 | [Iceberg CDC Pipeline](capstone-2-iceberg-cdc.md) | Phases 4-5 (Iceberg, streaming, ETL) |
| 3 | [BI Acceleration](capstone-3-bi-acceleration.md) | Phases 2, 4, 5 (marts, joins, Superset/Thrift) |

**Setup**: Run the data generation SparkApplication with `--scale medium` (see environment/README.md)
gives more realistic behavior than `small`. Capstone 2 needs the Iceberg jar
(`ENABLE_ICEBERG=1`, see `exercise-33`).

**Deliverable for each**: a short write-up (in `notes/`) with before/after metrics
from the Spark UI / History Server, the change you made, and why.
