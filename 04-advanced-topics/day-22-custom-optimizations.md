# Day 22: Custom Optimizations & Catalyst Rules

## 🎯 Learning Objectives
- Understand the full Catalyst pipeline and name the extension point at each stage
- Recognize the built-in rules that already fire on every query, so you don't reinvent them
- Use `SparkSessionExtensions` to inject an analyzer rule, an optimizer rule, and a planner strategy
- Read `explain(True)` well enough to prove a custom rule actually changed the plan
- Know the cheaper, config-level alternatives before writing custom Catalyst code

## 📚 Core Concepts

### 1. The Catalyst pipeline (where extension points live)

Every DataFrame/SQL query passes through the same five stages before a single task runs:

```
SQL string / DataFrame API
   → Parsed Logical Plan     (ANTLR-based parser turns text into an unresolved tree)
   → Analyzed Logical Plan   (Analyzer rules resolve columns, tables, types, functions)
   → Optimized Logical Plan  (Optimizer rule batches: pushdown, pruning, folding, join reorder)
   → Physical Plans          (Strategies translate one logical plan into one or more candidate physical plans)
   → Selected Physical Plan  (cost model + preparation rules pick and finalize one plan → RDDs)
```

**Key Points:**
- The Analyzer needs the **catalog** (table/column metadata) to resolve names — this is why an unregistered path-based DataFrame behaves differently from a catalog table in later stages (Day 26, Day 28).
- The Optimizer runs its rule batches to a **fixed point** (repeats until no rule changes the plan, or `spark.sql.optimizer.maxIterations` — default 100 — is hit).
- "Strategies" is the Catalyst term for the rules that turn a logical operator (e.g. `Join`) into a physical operator (e.g. `SortMergeJoinExec`, `BroadcastHashJoinExec`).
- Whole-stage code generation happens *after* physical plan selection, fusing adjacent operators into one generated Java method (the `*(N)` markers in `explain()`).

**Example:**
```python
df = spark.sql("SELECT customer_id, amount FROM transactions WHERE amount > 0")
df.explain(True)
# Prints: == Parsed Logical Plan == / == Analyzed Logical Plan == /
#         == Optimized Logical Plan == / == Physical Plan ==
# Read all four to see exactly what each stage contributed.
```

### 2. Built-in rules you already get for free

Before writing anything custom, know what Catalyst already does on every query. These are real rule classes you'll see referenced in plan diffs and Spark source:

| Rule | What it does |
|---|---|
| `ConstantFolding` | Evaluates constant expressions at plan time (`1 + 1` → `2`) |
| `BooleanSimplification` | Simplifies boolean expressions (`x AND true` → `x`) |
| `PushDownPredicate` | Moves filters below joins/projects/aggregates toward the scan |
| `ColumnPruning` | Removes unreferenced columns as early as possible |
| `CombineFilters` | Merges consecutive `Filter` nodes into one |
| `CollapseProject` | Merges consecutive `Project` (select) nodes into one |
| `NullPropagation` | Replaces expressions that provably evaluate to `NULL` |
| `EliminateOuterJoin` | Downgrades an outer join to inner when a later filter makes it safe |
| `ReorderJoin` / CBO join reorder | Reorders multi-way joins to reduce intermediate size (Day 28) |
| `PropagateEmptyRelation` | Short-circuits a branch that provably produces zero rows |

**Key Points:**
- These rules live in `org.apache.spark.sql.catalyst.optimizer` and are grouped into named **batches** (e.g. "Operator Optimization before Inferring Filters", "Join Reorder").
- Most "I wish Spark simplified this expression" requests are already handled — verify with `explain(True)` before assuming you need a custom rule.

### 3. Extension points

| Inject | Method | Use for |
|--------|--------|---------|
| Parser | `injectParser` | custom SQL syntax the built-in parser rejects |
| Analyzer (resolution) | `injectResolutionRule` | custom resolution logic run alongside built-in resolution |
| Analyzer (post-hoc) | `injectPostHocResolutionRule` | runs once after the plan is fully resolved |
| Analyzer (check) | `injectCheckRule` | validation that should fail the query (not rewrite it) |
| Optimizer | `injectOptimizerRule` | rewrite logical plans (domain-specific simplifications) |
| Planner strategy | `injectPlannerStrategy` | contribute custom physical operators |
| Function registry | `injectFunction` | register a custom built-in-style SQL function |

**Key Points:**
- All of these are registered through one `SparkSessionExtensions => Unit` entry point, set via `spark.sql.extensions`, and applied to **every session** that loads that configuration.
- Order matters within a category — Spark appends your rule to the existing batch/list; it doesn't let you insert it mid-batch by default.

### 4. A realistic use case: row-level security filter injection

The one custom-rule scenario that genuinely recurs across organizations is **row-level security (RLS)**: every query against a sensitive table must silently gain a `WHERE tenant_id = current_tenant()`-style filter, regardless of what the user wrote. This is a legitimate `injectOptimizerRule`/`injectPostHocResolutionRule` use case because no config lever (CBO/AQE/bucketing/DPP) can inject a *semantic* constraint — they only make existing plans faster.

```scala
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.expressions.EqualTo
import org.apache.spark.sql.catalyst.rules.Rule

object InjectTenantFilter extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case scan if isSensitiveTable(scan) && !alreadyFiltered(scan) =>
      Filter(EqualTo(tenantIdColumn(scan), currentTenantLiteral()), scan)
  }
}
```

**Key Points:**
- This kind of rule must be **idempotent** (re-applying it to already-filtered plan must be a no-op — see Issue 3) and must run early enough (analyzer/post-hoc-resolution stage) that no later optimizer rule can push a user filter *above* it in a way that bypasses the security constraint.
- Prefer `injectCheckRule` alongside it to **fail** the query outright if the security filter couldn't be attached, rather than silently returning unfiltered data.

### 5. When NOT to write a custom rule

99% of "I need a custom optimization" is solved by: correct **stats** (CBO, Day 28), **AQE** (Day 25), **bucketing** (Day 26), **DPP** (Day 27), or just rewriting the query. Custom Catalyst rules are powerful but a maintenance burden — the internal APIs (`LogicalPlan`, `Rule[LogicalPlan]`, expression trees) are **not** a stable public API and can change between minor Spark versions. Reach for custom rules only for genuine, repeated, framework-level rewrites that no config lever covers — row-level security (above) is the textbook legitimate case; rewriting a proprietary function call into Spark-native expressions is another.

## 🔍 Deep Dive: Injecting rules end-to-end (Scala)

### Step-by-Step Process

1. **Identify the stage.** Decide whether the rewrite belongs at parse time, resolution time, optimization time, or physical planning time. Most "add a filter" or "rewrite an expression" cases are optimizer rules.
2. **Write the `Rule[LogicalPlan]`.** Pattern-match on the plan tree using `transform`/`transformAllExpressions` and return an equivalent-but-better plan.
3. **Register it via `SparkSessionExtensions`.** Package it in a class implementing `SparkSessionExtensions => Unit`.
4. **Ship the jar and set the config.** `--jars myrules.jar --conf spark.sql.extensions=com.acme.MyExtensions`.
5. **Verify with `explain(True)`.** Compare the optimized plan with and without the extension loaded — if the plan is identical, the rule silently didn't match anything.

### Example: a logical optimizer rule

```scala
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.expressions.{Add, Literal}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.SparkSessionExtensions

// A trivial rule: rewrite `x + 0` to `x` (illustrative — Catalyst's own
// ConstantFolding/NullPropagation already cover this; a *real* custom rule
// would encode a domain rewrite Spark has no way to know about).
object RemovePlusZero extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case Add(left, Literal(0, _), _) => left
  }
}

class MyExtensions extends (SparkSessionExtensions => Unit) {
  def apply(ext: SparkSessionExtensions): Unit = {
    ext.injectOptimizerRule(_ => RemovePlusZero)
  }
}
// spark-submit --jars myrules.jar --conf spark.sql.extensions=com.acme.MyExtensions
```

### Example: a planner strategy (physical operator)

```scala
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.SparkPlan

// Strategies return Seq[SparkPlan]; an empty Seq means "I don't handle this",
// letting Spark fall through to its built-in strategies.
class NoOpStrategy(spark: SparkSession) extends Strategy {
  def apply(plan: LogicalPlan): Seq[SparkPlan] = Seq.empty
}

class MyExtensions2 extends (SparkSessionExtensions => Unit) {
  def apply(ext: SparkSessionExtensions): Unit = {
    ext.injectPlannerStrategy(session => new NoOpStrategy(session))
  }
}
```

### Inspecting rule effects

```python
df = spark.sql("SELECT amount + 0 FROM transactions")
df.explain(True)      # compare optimized plan with/without the extension loaded

# Turn on Catalyst's own rule-tracing to see every rule that actually fired
spark.conf.set("spark.sql.planChangeLog.level", "WARN")
df.explain(True)      # driver logs now show a before/after diff per rule that changed the plan
```

**Analysis:**
- If `spark.sql.planChangeLog.level` shows no entry for your rule, it never matched — check your pattern, not your registration.
- A rule that fires on *every* iteration but never stabilizes will hit `spark.sql.optimizer.maxIterations` and log a warning — a sign the rule isn't idempotent (see Issue 3 below).

## 💡 Key Insights for On-Premise

### 1. Extensions ship as a jar, not a notebook cell

Package your extension, put the jar on the classpath (`--jars` or bake it into the Spark Operator image), and set `spark.sql.extensions` in the `SparkApplication` manifest's `sparkConf`. On a shared on-prem cluster this config affects **every** session that inherits it — coordinate with the platform team before rolling it out cluster-wide, and consider gating it behind a per-job opt-in config rather than a cluster default.

```yaml
# environment/k8s SparkApplication snippet
spec:
  sparkConf:
    "spark.sql.extensions": "com.acme.MyExtensions"
  deps:
    jars:
      - "local:///opt/spark/extra-jars/myrules.jar"
```

### 2. PySpark can't author rules, but can consume them

Catalyst rules are JVM code operating on JVM expression trees. From PySpark you *consume* an extension jar (load it via `spark.sql.extensions`, same as Scala/Java jobs) — you don't write `Rule[LogicalPlan]` subclasses in Python. For Python-level business logic, prefer built-ins first, then Pandas UDFs (Day 24); don't reach for a JVM extension just to avoid a UDF.

### 3. Version pinning matters more than usual

Because `LogicalPlan`/`Rule`/expression internals aren't a stable API, a custom extension jar built against Spark 3.3 can break silently (or loudly, with a `NoSuchMethodError`) after a Spark upgrade. Track your Spark version in the extension jar's own version string, and re-test extensions as part of any Spark upgrade rollout on the cluster.

## 🎯 Practical Exercises

### Exercise 1: Watch Catalyst optimize (see `exercises/advanced/exercise-22-catalyst-rules.py`)
```python
from pyspark.sql import functions as F

txns.createOrReplaceTempView("txns_v")

# 1. Constant folding & boolean simplification
spark.sql("SELECT amount + 0 AS a, 1 = 1 AS always_true FROM txns_v").explain(True)

# 2. Predicate pushdown + column pruning into the Parquet scan
(txns.select("customer_id", "amount", "status")
     .where(F.col("status") == "active")
     .explain())
# -> In the physical plan, check PushedFilters and the reduced ReadSchema.
```

### Exercise 2: Decide build-vs-config
```python
# For each "I wish Spark did X" scenario below, name the cheapest lever
# BEFORE reaching for a custom rule, and justify it in one sentence:
#   1. "Join order picks a bad plan"        -> CBO + ANALYZE TABLE (Day 28)
#   2. "200 shuffle partitions, mostly tiny" -> AQE coalesce (Day 25)
#   3. "Same join runs every hour"          -> bucketing (Day 26)
#   4. "Star join scans the whole fact"     -> DPP (Day 27)
#   5. "Every query must apply tenant_id = current_user()" -> genuinely a
#      candidate for injectOptimizerRule/injectCheckRule (row-level security)
```

### Exercise 3: Diff the plan-change log
```python
# 1. Enable Catalyst's rule-tracing:
spark.conf.set("spark.sql.planChangeLog.level", "WARN")

# 2. Run a query with a few obviously foldable/simplifiable expressions:
spark.sql("""
    SELECT amount + 0 AS a, 1 = 1 AS t, customer_id
    FROM transactions
    WHERE status = 'active' AND true
""").explain(True)

# 3. In the driver logs, list every rule name that reported a plan change.
#    For each one, write one sentence describing what it simplified.
```

## 📊 Monitoring & Analysis

### Key Metrics to Monitor
1. **Logical-plan node count before/after** a rule — a custom rule should *reduce* nodes or replace expensive operators, not add hidden cost.
2. **Rule iteration count** — watch driver logs for `"...exceeded maxIterations..."`, a sign of a non-converging rule.
3. **Physical plan operator choice** — confirm a strategy injection produced the expected operator, not a silent fallback.
4. **Query compile time** (time from `explain()` call to plan availability) — heavy custom rules run on every query and add to planning latency, which matters for high-QPS Thrift Server workloads (Day 29).

### Spark UI Analysis
- **SQL tab**: open the query's plan viewer and confirm the rewritten plan matches your intent and doesn't regress (no new `Exchange`, no lost pushdown).
- **Driver logs**: with `spark.sql.planChangeLog.level=WARN`, each rule that changes the plan logs a diff — this is the fastest way to prove your rule fired versus silently no-opped.

## 🚨 Common Issues & Solutions

### Issue 1: Custom rule silently not applied
**Symptom**: Plan is identical with and without the extension loaded.
**Root Cause**: `spark.sql.extensions` not set, jar not on the classpath/image, the rule was registered but its pattern never matched the plan shape, or the rule ran but returned the plan unchanged.
**Solution**: Confirm the jar is loaded (`spark.conf.get("spark.sql.extensions")`), then enable `spark.sql.planChangeLog.level=WARN` and check whether your rule's name appears in the log at all. If it doesn't appear, registration failed; if it appears with no diff, the pattern match failed.

### Issue 2: Rule breaks correctness
**Symptom**: Wrong results after adding a rule — nulls handled incorrectly, or a rewrite that isn't valid for all input types.
**Root Cause**: Rules must preserve semantics for *every* possible input (nulls, type coercions, empty relations), not just the case you tested.
**Solution**: Write unit tests against the rule directly (construct a `LogicalPlan`, apply the rule, assert equivalence) covering null and boundary cases before shipping. This is exactly why config-level levers (CBO/AQE/bucketing/DPP) are preferred — Spark's own engineers already did this hardening for you.

### Issue 3: Rule doesn't converge (maxIterations exceeded)
**Symptom**: Driver log warning about exceeding `spark.sql.optimizer.maxIterations` (default 100); planning time balloons.
**Root Cause**: The rule isn't idempotent — applying it to its own output produces a different (but equivalent) plan forever, so the fixed-point loop never stabilizes.
**Solution**: Make the rule a true fixed point — it must return an unchanged plan when applied to its own output. Add a guard condition (e.g. check a marker/tag) so the second application is a no-op.

### Issue 4: Custom rule adds latency to every query
**Symptom**: All queries get slightly slower after the extension is installed cluster-wide, even ones the rule doesn't apply to.
**Root Cause**: `injectOptimizerRule` runs the rule on **every** query plan in that session, even if it never matches — an expensive pattern match (e.g. deep tree traversal, external lookups) becomes a tax on all workloads.
**Solution**: Make the rule's early-exit check as cheap as possible (a single top-level type check before recursing), and avoid I/O or external calls inside a Catalyst rule — it runs on the driver, synchronously, in the query-compile path.

### Issue 5: Extension breaks after a Spark version upgrade
**Symptom**: `NoSuchMethodError`/`ClassNotFoundException` when the extension jar loads on a newly upgraded cluster.
**Root Cause**: Catalyst internals (`LogicalPlan`, expression case classes) are not covered by Spark's public API compatibility guarantees and can change between minor versions.
**Solution**: Recompile and re-test the extension jar against the target Spark version as a mandatory step of any cluster Spark upgrade; keep the extension's own version tag aligned with the Spark version it was built against.

## 📝 Key Takeaways
1. Catalyst has five stages (parse → analyze → optimize → plan → select) with a documented `SparkSessionExtensions` hook at each.
2. Most simplifications you'd want (constant folding, pushdown, pruning) already exist as named built-in rules — check `explain(True)` before writing one yourself.
3. Prefer CBO/AQE/bucketing/DPP/query rewrites before custom Catalyst rules.
4. Custom rules are JVM jars applied cluster/session-wide via `spark.sql.extensions` — PySpark can consume, not author, them.
5. Rules must be idempotent and semantics-preserving for all inputs — test hard, and expect internal-API breakage across Spark upgrades.
6. Use `explain(True)` plus `spark.sql.planChangeLog.level=WARN` to verify a rule actually fired.

## 🔗 Next Steps
- **Day 23**: Advanced SQL & Window Functions
- Practice: read one production query's optimized plan and name every built-in rule that fired.

## 📚 Additional Resources
- Spark SQL `SparkSessionExtensions` API (`org.apache.spark.sql.SparkSessionExtensions`)
- Catalyst optimizer source: `org.apache.spark.sql.catalyst.optimizer.Optimizer` (rule batches)
- Spark configuration reference: `spark.sql.optimizer.maxIterations`, `spark.sql.planChangeLog.level`

---

**Progress**: Day 22/40 ✅
