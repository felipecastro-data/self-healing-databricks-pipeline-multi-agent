"""broken_scenarios/bad_join.py — failure taxonomy category: bad_join.

Originally joined orders_raw to itself on o_orderstatus — a low-cardinality,
non-unique key (only 3 distinct values, 'O'/'F'/'P') — without deduplicating
either side first, so every row on the left matched every row on the right
sharing the same status, exploding the output to roughly 2,000x the input
row count (duplicate-key row explosion, not a full Cartesian product).

Fixed by the self-healing pipeline (patch-proposer's bad_join remediation,
applied via apply_patch): the right side of the join is now deduplicated on
o_orderstatus and its uniqueness checked before the join runs, so each left
row matches at most one right row per status. The join still targets the
original o_orderstatus key — only the multiplication is eliminated. The
row-count check further below (against MAX_EXPLOSION_FACTOR) remains in
place as a guard against a regression of this same defect.

This matches the `bad_join` row of the failure taxonomy in CLAUDE.md.
Writes to <schema>.orders_summary_bad_join.
"""

from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from self_healing.jobs.checks import check_row_count_ratio, check_unique_key
from self_healing.jobs.json_log import get_logger

LOG = get_logger(__name__)


# If the join produces more than this multiple of the input row count,
# treat it as a row-explosion failure rather than a legitimate result.
MAX_EXPLOSION_FACTOR = 10


def run(spark: SparkSession, schema: str) -> None:
    orders = spark.table(f"{schema}.orders_raw")
    input_count = orders.count()

    # Fix: o_orderstatus is non-unique (only 3 distinct values), so the b
    # side is deduplicated on it and its uniqueness checked before the join
    # — each left row now matches at most one right row per status, instead
    # of every row in that status group (the row-explosion this used to
    # cause), while still joining on the original o_orderstatus key.
    orders_b = orders.dropDuplicates(["o_orderstatus"])
    check_unique_key(
        orders_b.count(), orders_b.select("o_orderstatus").distinct().count(), "join_key.o_orderstatus.unique"
    )
    joined = orders.alias("a").join(
        orders_b.alias("b"),
        on=F.col("a.o_orderstatus") == F.col("b.o_orderstatus"),
    )
    output_count = joined.count()

    LOG.info("join_row_counts", extra={"fields": {"input_rows": input_count, "output_rows": output_count}})

    # Databricks won't fail this join on its own, so make a row-count
    # explosion a hard job failure with a typed signature.
    check_row_count_ratio(input_count, output_count, MAX_EXPLOSION_FACTOR)

    summary = joined.groupBy("a.o_custkey").agg(
        F.sum("a.o_totalprice").alias("total_value"),
        F.count("a.o_orderkey").alias("order_count"),
    )
    summary.write.mode("overwrite").saveAsTable(f"{schema}.orders_summary_bad_join")
