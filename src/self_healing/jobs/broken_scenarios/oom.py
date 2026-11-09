"""broken_scenarios/oom.py — failure taxonomy category: oom.

Forces an unbounded shuffle/memory operation: cross-joins orders_raw against
itself with no join key, then collect()s the entire result to the driver.
With no join predicate, Spark must materialize the full Cartesian product
(row_count(orders_raw) ** 2 rows) and collect() pulls all of it into driver
memory at once instead of processing it distributed — exactly what triggers
OutOfMemoryError / executor lost / spill warnings on a real cluster.

NOTE — documented, expected behavior difference, not a bug: orders_raw in
this workspace is a small demo table (~5k rows), so a classic driver
OutOfMemoryError may not always reproduce, and on Databricks Free Edition
serverless compute in particular, the failure is more likely to surface as a
query timeout or a resource/size-limit error from the serverless compute
manager rather than a literal java.lang.OutOfMemoryError from a driver JVM.
Both should be treated as the same `oom` failure category for classification
purposes — the log-parser/classifier agents key off "OutOfMemoryError",
"executor lost", and "spill" signatures per CLAUDE.md, and will need to also
recognize serverless resource-limit/timeout messages in this environment.

Writes nothing on the expected failure path — self_healing_demo.pipeline.
orders_summary_oom is only written if the collect() somehow completes.

Runs as a standalone Databricks job (not a notebook). `spark` is injected
into the global namespace by the job cluster's runtime context; do not
import or instantiate a SparkSession here.
"""

SOURCE_TABLE = "self_healing_demo.pipeline.orders_raw"
TARGET_TABLE = "self_healing_demo.pipeline.orders_summary_oom"


def run() -> None:
    orders = spark.table(SOURCE_TABLE)
    other = spark.table(SOURCE_TABLE)

    # Defect: no join key at all -> full Cartesian product, growing as
    # row_count(orders) * row_count(other) instead of a bounded 1:1 match.
    cartesian = orders.crossJoin(other)

    # Defect: collect() pulls the entire (unbounded) result set onto the
    # driver as Python objects instead of writing it out distributed. This
    # is the actual OOM trigger — crossJoin alone is lazy/cheap; collect()
    # is what forces every row of the Cartesian product into driver memory
    # at once.
    collected_rows = cartesian.collect()

    # Unreachable on a real-sized dataset / real cluster: the collect()
    # above is expected to fail (or time out / hit a serverless resource
    # limit) first. Kept only so the script has a defined "if it somehow
    # completes" path, per the scenario spec.
    result_df = spark.createDataFrame(collected_rows, cartesian.schema)
    result_df.write.mode("overwrite").saveAsTable(TARGET_TABLE)


if __name__ == "__main__":
    run()
