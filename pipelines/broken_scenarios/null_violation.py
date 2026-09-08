"""broken_scenarios/null_violation.py — failure taxonomy category: null_violation.

Aggregates orders_raw the same way as golden_job.py, but deliberately nulls
out total_value for every customer with 3 or fewer orders (as if an upstream
enrichment step — e.g. a currency-conversion lookup — silently failed for
those rows; the real source data has no nulls in o_totalprice, this null is
injected here). The result is written into a Delta table whose total_value
column has an explicit NOT NULL constraint, so the write fails with
"[DELTA_NOT_NULL_CONSTRAINT_VIOLATED] NOT NULL constraint violated for
column: total_value" (verified against a live Delta table in this
workspace).

This matches the `null_violation` row of the failure taxonomy in CLAUDE.md.
Writes to self_healing_demo.pipeline.orders_summary_null_violation.

Runs as a standalone Databricks job (not a notebook). `spark` is injected
into the global namespace by the job cluster's runtime context; do not
import or instantiate a SparkSession here.
"""

from pyspark.sql import functions as F

SOURCE_TABLE = "self_healing_demo.pipeline.orders_raw"
TARGET_TABLE = "self_healing_demo.pipeline.orders_summary_null_violation"


def ensure_target_table() -> None:
    """Idempotently create the target table with total_value NOT NULL."""
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            o_custkey BIGINT,
            total_value DECIMAL(18,2) NOT NULL,
            order_count BIGINT
        )
        USING DELTA
        """
    )


def run() -> None:
    ensure_target_table()

    orders = spark.table(SOURCE_TABLE)

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

    # insertInto (not saveAsTable) writes into the existing table using its
    # existing schema, so the NOT NULL constraint created above is actually
    # enforced. saveAsTable("overwrite") would instead recreate the table
    # from the DataFrame's schema and silently drop the constraint.
    summary_with_nulls.write.insertInto(TARGET_TABLE, overwrite=True)


if __name__ == "__main__":
    run()
