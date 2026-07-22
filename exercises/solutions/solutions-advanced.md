# Solutions — Advanced (Days 22-28)

## exercise-22 (Catalyst rules)
1. Constant folding + simplification (`amount+0`, `1=1`), predicate pushdown, and column
   pruning all fire in the shown plans.
2. Cheapest levers: CBO (join order), AQE (partitions/skew), bucketing (repeated key
   joins), DPP (star-join fact pruning), or a query rewrite — before any custom rule.

## exercise-23 (advanced SQL)
1. A single `Window` operator handles all three reused columns (one shuffle+sort).
2. The `partitionBy` key (`customer_id`/`category`) drives skew risk.
3. `rollup` computes date, category, and grand totals in one scan+shuffle vs 3 groupBys.

## exercise-24 (UDF performance)
1. Built-in → neither eval node; Python UDF → `BatchEvalPython`; Pandas UDF → `ArrowEvalPython`.
2. At scale: built-in fastest, Pandas UDF close, Python UDF markedly slowest.
3. Only the built-in is expressed in Catalyst, so it gets codegen + pushdown; UDFs are opaque.

## exercise-25 (broadcast & AQE)
1. Enabling broadcast removes the `Exchange` nodes feeding the join (SMJ → BroadcastHashJoin).
2. AQE coalesces the 200 configured partitions down to a handful (see `AQEShuffleRead`).
3. Broadcasting is dangerous when the "small" side isn't small → driver/executor OOM.

## exercise-26 (bucketing)
1. The 16⨝16 bucketed join has no `Exchange`; matching bucket layout co-locates keys.
2. Counts must match so the same key hashes to the same bucket index on both sides.
3. Worth it when the table is joined/aggregated on that key many times (amortizes the
   one-time write shuffle). (Skipped locally without Hive — expected.)

## exercise-27 (DPP)
1. The fact `Scan` shows a `dynamicpruningexpression` / dynamic `PartitionFilters`.
2. DPP on reads far fewer partitions than DPP off (compare "partitions read").
3. Broadcasting the dimension makes its join keys cheaply available to inject as the
   runtime partition filter.

## exercise-28 (CBO)
1. With column stats, estimated rows should be close to actual; large gaps = stale stats.
2. CBO on may reorder the 3-way join to keep intermediates smaller.
3. Without column statistics CBO has no cost model → no reordering.
   (Needs a catalog; skipped locally with a note — expected.)
