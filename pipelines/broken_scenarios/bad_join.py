"""broken_scenarios/bad_join.py — failure taxonomy category: bad_join.

Joins orders_raw to itself on o_orderstatus — a low-cardinality, non-unique
key (verified against live data: only 3 distinct values, 'O'/'F'/'P') —
without deduplicating either side first. Every row on the left matches every
row on the right that shares the same status, so the join multiplies row
counts within each status group instead of matching 1:1 (duplicate-key row
explosion, not a clean Cartesian product across the whole table).

Databricks itself won't raise a hard error for a row-count explosion — a big
join is still a "valid" query — so this script makes the failure explicit:
it prints input/output row counts (visible in the run output even if the
job doesn't hard-fail) and asserts the output stays under a sane multiple of
the input, raising if it doesn't. Against this workspace's actual orders_raw
distribution, the join produces roughly 2,000x the input row count, so the
assertion is expected to fail reliably.

This matches the `bad_join` row of the failure taxonomy in CLAUDE.md.
Writes to self_healing_demo.pipeline.orders_summary_bad_join.

Runs as a standalone Databricks job (not a notebook). `spark` is injected
into the global namespace by the job cluster's runtime context; do not
import or instantiate a SparkSession here.
"""

from pyspark.sql import functions as F

SOURCE_TABLE = "self_healing_demo.pipeline.orders_raw"
TARGET_TABLE = "self_healing_demo.pipeline.orders_summary_bad_join"

# If the join produces more than this multiple of the input row count,
# treat it as a row-explosion failure rather than a legitimate result.
MAX_EXPLOSION_FACTOR = 10


def run() -> None:
    orders = spark.table(SOURCE_TABLE)
    input_count = orders.count()

    # Defect: joining on o_orderstatus (non-unique, only 3 distinct values)
    # without deduplicating first. Every row matches every other row that
    # shares the same status -> quadratic blowup within each status group.
    joined = orders.alias("a").join(
        orders.alias("b"),
        on=F.col("a.o_orderstatus") == F.col("b.o_orderstatus"),
    )
    output_count = joined.count()

    print(f"[bad_join] input row count: {input_count}")
    print(f"[bad_join] output row count after join: {output_count}")
    print(f"[bad_join] explosion factor: {output_count / input_count:.1f}x")

    # Databricks won't fail this join on its own, so make the row-count
    # explosion a hard job failure with an explicit signature.
    assert output_count <= input_count * MAX_EXPLOSION_FACTOR, (
        f"row explosion detected: {output_count} output rows from "
        f"{input_count} input rows (>{MAX_EXPLOSION_FACTOR}x) — "
        f"likely a duplicate-key / non-unique join key defect"
    )

    summary = joined.groupBy("a.o_custkey").agg(
        F.sum("a.o_totalprice").alias("total_value"),
        F.count("a.o_orderkey").alias("order_count"),
    )
    summary.write.mode("overwrite").saveAsTable(TARGET_TABLE)


if __name__ == "__main__":
    run()
