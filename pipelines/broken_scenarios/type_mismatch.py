"""broken_scenarios/type_mismatch.py — failure taxonomy category: type_mismatch.

Runs the same aggregation as golden_job.py, but additionally computes a flag
column that compares o_totalprice (DECIMAL(18,2)) directly against
o_orderdate (DATE) with no explicit cast. Spark has no implicit conversion
between DECIMAL and DATE, so resolving this expression raises an
AnalysisException at DataFrame-resolution time —
"[DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES] Cannot resolve ... due to data
type mismatch" (verified live against this workspace's SQL engine) — the
exact "cannot resolve / implicit cast error" signature in the
`type_mismatch` row of the failure taxonomy in CLAUDE.md. This fails at
plan-analysis time, before any data is read, so it is deterministic
regardless of the actual contents of orders_raw.

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

    # Defect: comparing a DECIMAL column directly against a DATE column with
    # no explicit .cast(). There is no implicit DECIMAL<->DATE conversion in
    # Spark, so resolving this column raises AnalysisException as soon as
    # it's referenced below.
    flagged = orders.withColumn(
        "price_equals_order_date",
        F.col("o_totalprice") == F.col("o_orderdate"),
    )

    summary = flagged.groupBy("o_custkey").agg(
        F.sum("o_totalprice").alias("total_value"),
        F.count("o_orderkey").alias("order_count"),
    )

    summary.write.mode("overwrite").saveAsTable(TARGET_TABLE)


if __name__ == "__main__":
    run()
