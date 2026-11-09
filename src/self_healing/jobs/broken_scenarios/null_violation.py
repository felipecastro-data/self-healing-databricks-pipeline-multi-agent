"""broken_scenarios/null_violation.py — failure taxonomy category: null_violation.

Aggregates orders_raw the same way as golden_job.py, but deliberately nulls
out total_value for every customer with 3 or fewer orders (as if an upstream
enrichment step — e.g. a currency-conversion lookup — silently failed for
those rows; the real source data has no nulls in o_totalprice, this null is
injected here). contracts/orders_summary.yml declares total_value not
nullable, so the job stops before writing with a DataQualityError citing
clause columns.total_value.not_null. Behind that check the target Delta
table still has its NOT NULL constraint; before the contract existed, this
job failed at the write with "[DELTA_NOT_NULL_CONSTRAINT_VIOLATED]" (see
evidence/logs/null_violation_before.txt).

This matches the `null_violation` row of the failure taxonomy in CLAUDE.md.
Writes to <schema>.orders_summary_null_violation.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from self_healing.jobs.contract import load_contract, validate_contract


def ensure_target_table(spark: SparkSession, target_table: str) -> None:
    """Idempotently create the target table with total_value NOT NULL."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_table} (
            o_custkey BIGINT,
            total_value DECIMAL(18,2) NOT NULL,
            order_count BIGINT
        )
        USING DELTA
        """
    )


def run(spark: SparkSession, schema: str) -> None:
    target_table = f"{schema}.orders_summary_null_violation"
    ensure_target_table(spark, target_table)

    orders = spark.table(f"{schema}.orders_raw")

    summary = orders.groupBy("o_custkey").agg(
        F.sum("o_totalprice").alias("total_value"),
        F.count("o_orderkey").alias("order_count"),
    )

    # Defect: simulate a broken upstream enrichment step that nulls out
    # total_value for low-order-count customers. Column order below must
    # stay aligned with the target table's column order for insertInto.
    summary_with_nulls = summary.withColumn(
        "total_value",
        F.when(F.col("order_count") > 3, F.col("total_value")).otherwise(F.lit(None)),
    ).select("o_custkey", "total_value", "order_count")

    summary_with_nulls = summary_with_nulls.na.drop(subset=["total_value"])
    validate_contract(summary_with_nulls, load_contract("orders_summary"))

    # insertInto (not saveAsTable) writes into the existing table using its
    # existing schema, so the NOT NULL constraint created above is actually
    # enforced. saveAsTable("overwrite") would instead recreate the table
    # from the DataFrame's schema and silently drop the constraint.
    summary_with_nulls.write.insertInto(target_table, overwrite=True)
