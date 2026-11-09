"""ingest_orders_raw.py: build orders_raw from the public samples.tpch.orders table.

This is the first step of the pipeline. It checks the source against
contracts/orders_raw.yml, then overwrites <schema>.orders_raw, so re-running
it is safe and always gives the same table.
"""

from __future__ import annotations

from pyspark.sql import SparkSession

from self_healing.jobs.contract import load_contract, validate_contract
from self_healing.jobs.json_log import get_logger

SOURCE_TABLE = "samples.tpch.orders"
LOG = get_logger(__name__)


def run(spark: SparkSession, schema: str, source_table: str = SOURCE_TABLE) -> None:
    """Validate source_table and write it to <schema>.orders_raw."""
    target_table = f"{schema}.orders_raw"
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")

    orders = spark.table(source_table)
    validate_contract(orders, load_contract("orders_raw"))
    orders.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(target_table)

    LOG.info("ingested", extra={"fields": {"source": source_table, "target": target_table, "rows": orders.count()}})
