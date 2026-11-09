"""golden_job.py: the known-good job every broken scenario deliberately deviates from.

Aggregates order value and order count per customer from <schema>.orders_raw
and overwrites <schema>.orders_summary. The input and output are both checked
against their contracts in contracts/ before anything is written.

Every module in broken_scenarios/ injects exactly one defect that produces a
log signature from the failure taxonomy in CLAUDE.md.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from self_healing.jobs.contract import load_contract, validate_contract
from self_healing.jobs.json_log import get_logger

LOG = get_logger(__name__)


def run(spark: SparkSession, schema: str) -> None:
    """Validate orders_raw, aggregate it, validate the result, and write it."""
    target_table = f"{schema}.orders_summary"
    orders = spark.table(f"{schema}.orders_raw")
    validate_contract(orders, load_contract("orders_raw"))

    summary = orders.groupBy("o_custkey").agg(
        F.sum("o_totalprice").alias("total_value"),
        F.count("o_orderkey").alias("order_count"),
    )
    validate_contract(summary, load_contract("orders_summary"))

    summary.write.mode("overwrite").saveAsTable(target_table)
    LOG.info("written", extra={"fields": {"target": target_table}})
