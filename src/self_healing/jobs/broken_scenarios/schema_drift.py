"""broken_scenarios/schema_drift.py — failure taxonomy category: schema_drift.

Originally referenced o_customer_email, a column that does not exist on
<schema>.orders_raw (verified against the live table
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
logs an explicit warning (visible in the job's captured output, not a
silent fallback) and continues with
customer_email set to NULL for all rows. The job now completes
successfully against this workspace's current orders_raw, which has no
email column.

Writes to <schema>.orders_summary_schema_drift.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from self_healing.jobs.json_log import get_logger

LOG = get_logger(__name__)


def run(spark: SparkSession, schema: str) -> None:
    orders = spark.table(f"{schema}.orders_raw")

    # Fix: o_customer_email does not exist in orders_raw. Rather than assert
    # it must be present (which only renamed the exception without resolving
    # the drift), branch on the table's actual schema so the job proceeds —
    # falling back to a NULL customer_email, with an explicit warning logged
    # so the degradation is visible in the job's output/logs.
    if "o_customer_email" in orders.columns:
        enriched = orders.withColumn("customer_email", F.col("o_customer_email"))
    else:
        LOG.warning(
            "missing_column_filled_with_null",
            extra={"fields": {"column": "o_customer_email", "available": orders.columns}},
        )
        enriched = orders.withColumn("customer_email", F.lit(None))

    summary = enriched.groupBy("o_custkey").agg(
        F.sum("o_totalprice").alias("total_value"),
        F.count("o_orderkey").alias("order_count"),
    )

    summary.write.mode("overwrite").saveAsTable(f"{schema}.orders_summary_schema_drift")
