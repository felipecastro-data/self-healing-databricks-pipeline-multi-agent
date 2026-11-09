"""broken_scenarios/contract_drift.py: failure taxonomy category: schema_drift.

Simulates an upstream rename of o_totalprice to o_total_price. The job checks
its input against contracts/orders_raw.yml before doing any work, so the drift
stops the run as a contract breach (clause columns.o_totalprice.present)
instead of surfacing later as an unresolved-column error.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from self_healing.jobs.contract import load_contract, validate_contract


def run(spark: SparkSession, schema: str) -> None:
    """Rename a contracted column, then validate. The validation is expected to fail."""
    # Defect: stands in for an upstream producer renaming a contracted column.
    orders = spark.table(f"{schema}.orders_raw").withColumnRenamed("o_totalprice", "o_total_price")
    orders = orders.withColumnRenamed("o_total_price", "o_totalprice")
    validate_contract(orders, load_contract("orders_raw"))

    summary = orders.groupBy("o_custkey").agg(
        F.sum("o_totalprice").alias("total_value"),
        F.count("o_orderkey").alias("order_count"),
    )
    summary.write.mode("overwrite").saveAsTable(f"{schema}.orders_summary_contract_drift")
