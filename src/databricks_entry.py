"""Entry point every Databricks job runs: imports a job module and passes it the session.

A spark_python_task executes this file directly, so the package isn't on the
path yet and `spark` exists only as a runtime global. This shim handles both,
which keeps every job module importable by tests.

Parameters come from databricks.yml:
    --src     workspace path of this src/ directory
    --job     module under self_healing.jobs, e.g. broken_scenarios.bad_join
    --schema  catalog.schema the job reads and writes
"""

import argparse
import importlib
import sys


def main(session: object) -> None:
    """Parse the job parameters and run the selected job."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--src", required=True)
    parser.add_argument("--job", required=True)
    parser.add_argument("--schema", required=True)
    args = parser.parse_args()

    sys.path.insert(0, args.src)
    importlib.import_module(f"self_healing.jobs.{args.job}").run(session, args.schema)


if __name__ == "__main__":
    main(spark)  # noqa: F821 - injected by the Databricks runtime
