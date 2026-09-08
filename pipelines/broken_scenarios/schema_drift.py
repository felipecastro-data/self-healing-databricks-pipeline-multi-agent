"""broken_scenarios/schema_drift.py — failure taxonomy category: schema_drift.

Runs the same aggregation as golden_job.py, but also references
o_customer_email, a column that does not exist on
self_healing_demo.pipeline.orders_raw (verified against the live table
schema — orders_raw has no email column). Referencing it raises an
AnalysisException at DataFrame-resolution time
("UNRESOLVED_COLUMN.WITH_SUGGESTION" — the modern spelling of the classic
"column not found" message), before any data is read or written.

This matches the `schema_drift` row of the failure taxonomy in CLAUDE.md:
a downstream consumer (or a schema change upstream) expects a column that
isn't actually present in the source table. Writes to
self_healing_demo.pipeline.orders_summary_schema_drift.

Runs as a standalone Databricks job (not a notebook). `spark` is injected
into the global namespace by the job cluster's runtime context; do not
import or instantiate a SparkSession here.
"""

from pyspark.sql import functions as F

SOURCE_TABLE = "self_healing_demo.pipeline.orders_raw"
TARGET_TABLE = "self_healing_demo.pipeline.orders_summary_schema_drift"


def run() -> None:
    orders = spark.table(SOURCE_TABLE)

    # Defect: o_customer_email does not exist in orders_raw. Referencing it
    # raises AnalysisException as soon as it's resolved below.
    enriched = orders.withColumn("customer_email", F.col("o_customer_email"))

    summary = enriched.groupBy("o_custkey").agg(
        F.sum("o_totalprice").alias("total_value"),
        F.count("o_orderkey").alias("order_count"),
    )

    summary.write.mode("overwrite").saveAsTable(TARGET_TABLE)


if __name__ == "__main__":
    run()
