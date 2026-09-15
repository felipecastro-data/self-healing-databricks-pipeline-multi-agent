"""broken_scenarios/bad_join.py — failure taxonomy category: bad_join.

Originally joined orders_raw to itself on o_orderstatus — a low-cardinality,
non-unique key (only 3 distinct values, 'O'/'F'/'P') — without deduplicating
either side first, so every row on the left matched every row on the right
sharing the same status, exploding the output to roughly 2,000x the input
row count (duplicate-key row explosion, not a full Cartesian product).

Fixed by the self-healing pipeline (patch-proposer's bad_join remediation,
applied via apply_patch): the right side of the join is now deduplicated on
o_orderstatus and its uniqueness asserted before the join runs, so each left
row matches at most one right row per status. The join still targets the
original o_orderstatus key — only the multiplication is eliminated. The
row-count assertion further below (against MAX_EXPLOSION_FACTOR) remains in
place as a guard against a regression of this same defect; against this
workspace's current orders_raw data, it no longer fires.

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

    # Fix: o_orderstatus is non-unique (only 3 distinct values), so the b
    # side is deduplicated on it and its uniqueness asserted before the join
    # — each left row now matches at most one right row per status, instead
    # of every row in that status group (the row-explosion this used to
    # cause), while still joining on the original o_orderstatus key.
    orders_b = orders.dropDuplicates(["o_orderstatus"])
    assert orders_b.count() == orders_b.select("o_orderstatus").distinct().count(), (
        "join key o_orderstatus is not unique on the b side after dedup"
    )
    joined = orders.alias("a").join(
        orders_b.alias("b"),
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
