"""broken_scenarios/type_mismatch.py — failure taxonomy category: type_mismatch.

Originally computed a flag column that compared o_totalprice
(DECIMAL(18,2)) directly against o_orderdate (DATE) with no explicit cast.
Spark has no implicit conversion between DECIMAL and DATE, so resolving
this expression raised an AnalysisException at DataFrame-resolution time —
"[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve ... due to data
type mismatch" — the "cannot resolve / implicit cast error" signature in
the `type_mismatch` row of the failure taxonomy in CLAUDE.md.

Fixed by the self-healing pipeline (patch-proposer's type_mismatch
remediation, applied via apply_patch) — but it took two diagnostic passes:
the first patch cast o_orderdate directly to decimal(18,2)
(`.cast("decimal(18,2)")`), which failed with a different error,
"[DATATYPE_MISMATCH.CAST_WITH_FUNC_SUGGESTION] ... cannot cast \"DATE\" to
\"DECIMAL(18,2)\"" — Spark has no direct DATE->DECIMAL cast path at all
(the platform error message itself suggests the `UNIX_DATE` function
instead). The second pass's fix instead chains three explicit `.cast()`
calls through a Spark-supported path: DATE -> TIMESTAMP -> LONG ->
DECIMAL(18,2), converting o_orderdate to its epoch-seconds representation
before comparing. This still fits the `type_mismatch` template
("explicit `.cast()` on offending column") without introducing a new
function call. The comparison itself remains semantically meaningless
(price vs. a date-derived number) — this scenario only exercises the
type-mismatch failure/remediation path, not real business logic.

Writes to self_healing_demo.pipeline.orders_summary_type_mismatch.

Runs as a standalone Databricks job (not a notebook). `spark` is injected
into the global namespace by the job cluster's runtime context; do not
import or instantiate a SparkSession here.
"""

from pyspark.sql import functions as F

SOURCE_TABLE = "self_healing_demo.pipeline.orders_raw"
TARGET_TABLE = "self_healing_demo.pipeline.orders_summary_type_mismatch"


def run() -> None:
    orders = spark.table(SOURCE_TABLE)

    # Fix: o_orderdate has no direct cast to decimal, so it's chained through
    # timestamp -> long (epoch seconds) -> decimal(18,2), a Spark-supported
    # cast path, instead of the unsupported direct DATE->DECIMAL cast tried
    # in the first patch attempt.
    flagged = orders.withColumn(
        "price_equals_order_date",
        F.col("o_totalprice") == F.col("o_orderdate").cast("timestamp").cast("long").cast("decimal(18,2)"),
    )

    summary = flagged.groupBy("o_custkey").agg(
        F.sum("o_totalprice").alias("total_value"),
        F.count("o_orderkey").alias("order_count"),
    )

    summary.write.mode("overwrite").saveAsTable(TARGET_TABLE)


if __name__ == "__main__":
    run()
