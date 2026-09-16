"""broken_scenarios/schema_drift.py — failure taxonomy category: schema_drift.

Originally referenced o_customer_email, a column that does not exist on
self_healing_demo.pipeline.orders_raw (verified against the live table
schema — orders_raw has no email column). Referencing it unconditionally
raised an AnalysisException at DataFrame-resolution time
("UNRESOLVED_COLUMN.WITH_SUGGESTION" — the modern spelling of the classic
"column not found" message), before any data is read or written. This
matches the `schema_drift` row of the failure taxonomy in CLAUDE.md: a
downstream consumer (or a schema change upstream) expects a column that
isn't actually present in the source table.

Fixed by the self-healing pipeline (patch-proposer's schema_drift
remediation, applied via apply_patch) — but it took two diagnostic passes.
The first pass (commit adc19f7, applied before the human-approval gate
existed) added an `assert "o_customer_email" in orders.columns` guard
ahead of the reference. That only renamed the exception from
AnalysisException to AssertionError; the column was still missing and the
job still failed, verified live on a subsequent run. The second pass was
run back through the full diagnostic chain with patch-proposer explicitly
told the assert-only fix was cosmetic and ruled out. The result branches
on the table's actual schema instead of asserting a fixed shape: if
o_customer_email is present it's used as before, and if absent the job
prints an explicit `[schema_drift] WARNING: ...` line (visible in the
job's captured output, not a silent fallback) and continues with
customer_email set to NULL for all rows. The job now completes
successfully against this workspace's current orders_raw, which has no
email column.

Writes to self_healing_demo.pipeline.orders_summary_schema_drift.

Runs as a standalone Databricks job (not a notebook). `spark` is injected
into the global namespace by the job cluster's runtime context; do not
import or instantiate a SparkSession here.
"""

from pyspark.sql import functions as F

SOURCE_TABLE = "self_healing_demo.pipeline.orders_raw"
TARGET_TABLE = "self_healing_demo.pipeline.orders_summary_schema_drift"


def run() -> None:
    orders = spark.table(SOURCE_TABLE)

    # Fix: o_customer_email does not exist in orders_raw. Rather than assert
    # it must be present (which only renamed the exception without resolving
    # the drift), branch on the table's actual schema so the job proceeds —
    # falling back to a NULL customer_email, with an explicit warning printed
    # so the degradation is visible in the job's output/logs.
    if "o_customer_email" in orders.columns:
        enriched = orders.withColumn("customer_email", F.col("o_customer_email"))
    else:
        print(
            f"[schema_drift] WARNING: column 'o_customer_email' not found in {SOURCE_TABLE} "
            f"(available columns: {orders.columns}); setting customer_email to NULL for all rows"
        )
        enriched = orders.withColumn("customer_email", F.lit(None))

    summary = enriched.groupBy("o_custkey").agg(
        F.sum("o_totalprice").alias("total_value"),
        F.count("o_orderkey").alias("order_count"),
    )

    summary.write.mode("overwrite").saveAsTable(TARGET_TABLE)


if __name__ == "__main__":
    run()
