# Day 22: Custom Optimizations & Catalyst Rules

## 🎯 Learning Objectives
- Understand where you *can* extend Catalyst (rules, strategies, extensions)
- Read a plan well enough to know when a custom rule is even warranted
- Use `SparkSessionExtensions` to inject an optimizer rule
- Know the cheaper alternatives before writing custom Catalyst code

## 📚 Core Concepts

### 1. The Catalyst pipeline (where extension points live)
```
SQL / DataFrame
   → Parsed Logical Plan     (parser)
   → Analyzed Logical Plan   (Analyzer rules — resolve columns/tables)
   → Optimized Logical Plan  (Optimizer rules — pushdown, constant folding, ...)
   → Physical Plans          (Strategies — choose joins, aggregates)
   → Selected Physical Plan  (cost + preparation rules → RDDs)
```
You can inject custom rules at several of these stages via `SparkSessionExtensions`.

### 2. Extension points
| Inject | Method | Use for |
|--------|--------|---------|
| Analyzer rule | `injectResolutionRule` | custom resolution / validation |
| Optimizer rule | `injectOptimizerRule` | rewrite logical plans (e.g. domain simplifications) |
| Planner strategy | `injectPlannerStrategy` | custom physical operators |
| Parser | `injectParser` | custom SQL syntax |

### 3. When NOT to write a custom rule
99% of "I need a custom optimization" is solved by: correct **stats** (CBO, Day 28), **AQE** (Day 25), **bucketing** (Day 26), **DPP** (Day 27), or just rewriting the query. Custom Catalyst rules are powerful but a maintenance burden — reach for them only for genuine, repeated, framework-level rewrites.

## 🔍 Deep Dive: Injecting an optimizer rule (Scala)

```scala
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.SparkSessionExtensions

// A trivial rule: rewrite `x + 0` to `x` (illustrative — Catalyst already does this).
object RemovePlusZero extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = plan transformAllExpressions {
    case org.apache.spark.sql.catalyst.expressions.Add(left, Literal(0, _), _) => left
  }
}

class MyExtensions extends (SparkSessionExtensions => Unit) {
  def apply(ext: SparkSessionExtensions): Unit =
    ext.injectOptimizerRule(_ => RemovePlusZero)
}
// spark-submit --conf spark.sql.extensions=com.acme.MyExtensions
```

### Inspecting rule effects
```python
df = spark.sql("SELECT amount + 0 FROM transactions")
df.explain(True)      # compare optimized plan with/without the extension
```

## 💡 Key Insights for On-Premise
### 1. Extensions ship as a jar
Package your extension, put the jar on the classpath (`--jars`), and set `spark.sql.extensions`. On a shared cluster this affects **every** session using that config — coordinate with the platform team.

### 2. PySpark can't author rules, but can use them
Catalyst rules are JVM code. From PySpark you *consume* an extension jar; you don't write rules in Python. For Python-level logic, prefer built-ins / Pandas UDFs (Day 24).

## 🎯 Practical Exercises

### Exercise 1: Watch Catalyst optimize
```python
# See exercises/advanced/exercise-22-catalyst-rules.py
# Observe built-in rules (constant folding, predicate pushdown, pruning) in explain output.
```

### Exercise 2: Decide build-vs-config
```python
# For three "I wish Spark did X" scenarios, pick the cheapest lever (CBO/AQE/bucket/DPP/rewrite).
```

## 📊 Monitoring & Analysis
### Key Metrics to Monitor
1. Logical-plan node count before/after a rule.
2. Physical plan operator choice changes.

### Spark UI Analysis
- SQL tab: confirm the rewritten plan matches your intent and doesn't regress.

## 🚨 Common Issues & Solutions

### Issue 1: Custom rule silently not applied
**Symptom**: plan unchanged.
**Solution**: `spark.sql.extensions` not set, jar not on classpath, or rule returned the plan unchanged (pattern didn't match).

### Issue 2: Rule breaks correctness
**Symptom**: wrong results after adding a rule.
**Solution**: rules must preserve semantics for *all* inputs — test with edge cases (nulls, types). This is why config-level levers are preferred.

## 📝 Key Takeaways
1. Catalyst has clear extension points via `SparkSessionExtensions`.
2. Prefer CBO/AQE/bucketing/DPP/rewrites before custom rules.
3. Rules are JVM jars applied via `spark.sql.extensions`.
4. Custom rules must preserve semantics — test hard.
5. Use `explain(True)` to verify rule effects.

## 🔗 Next Steps
- **Day 23**: Advanced SQL & Window Functions
- Practice: read one production query's optimized plan and name every rule that fired.

## 📚 Additional Resources
- Spark SQL `SparkSessionExtensions` API
- Catalyst optimizer design docs

---

**Progress**: Day 22/40 ✅
