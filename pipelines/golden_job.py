"""golden_job.py — reference "happy path" pipeline job for the self-healing pipeline demo.

Reads the raw orders table (self_healing_demo.pipeline.orders_raw), aggregates
total order value and order count per customer, and writes the result to
self_healing_demo.pipeline.orders_summary (overwrite mode).

Every script under pipelines/broken_scenarios/ is a deliberate, documented
deviation from this exact logic — each one injects exactly one defect that
produces a log signature matching one row of the failure taxonomy in
CLAUDE.md.

Runs as a standalone Databricks job (not a notebook). `spark` is injected
into the global namespace by the job cluster's runtime context; do not
import or instantiate a SparkSession here.
"""

from pyspark.sql import functions as F

SOURCE_TABLE = "self_healing_demo.pipeline.orders_raw"
TARGET_TABLE = "self_healing_demo.pipeline.orders_summary"


def run() -> None:
    orders = spark.table(SOURCE_TABLE)

    summary = orders.groupBy("o_custkey").agg(
        F.sum("o_totalprice").alias("total_value"),
        F.count("o_orderkey").alias("order_count"),
    )

    summary.write.mode("overwrite").saveAsTable(TARGET_TABLE)


if __name__ == "__main__":
    run()
