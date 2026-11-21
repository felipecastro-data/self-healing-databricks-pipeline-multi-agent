"""publish_evidence.py: the serving stage. Loads the evidence files into Delta tables.

Reads evidence/run_history.json and evidence/run_journal.jsonl from the Git
folder the job runs from, and overwrites two tables that
dashboards/results.lvdash.json reads:

- <schema>.evidence_runs: one row per job run
- <schema>.evidence_rounds: one row per diagnostic round, grouped by
  self_healing.journal.build_rounds, the same logic as docs/results.md
"""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import SparkSession

from self_healing import journal
from self_healing.jobs.json_log import get_logger

LOG = get_logger(__name__)

RUNS_DDL = "job_name STRING, job_run_id BIGINT, start_time TIMESTAMP, result_state STRING, run_duration_s INT"
ROUNDS_DDL = (
    "started TIMESTAMP, scenario STRING, category STRING, clause STRING, diagnosis_s INT, "
    "applied BOOLEAN, outcome STRING, verify_s INT"
)


def run(spark: SparkSession, schema: str, evidence_dir: Path = journal.REPO_ROOT / "evidence") -> None:
    """Overwrite <schema>.evidence_runs and <schema>.evidence_rounds from the evidence files."""
    history = json.loads((evidence_dir / "run_history.json").read_text())
    lines = (evidence_dir / "run_journal.jsonl").read_text().splitlines()
    rounds = journal.build_rounds([json.loads(line) for line in lines if line.strip()])

    runs = [
        (
            r["job_name"],
            r["job_run_id"],
            datetime.fromisoformat(r["start_time"]),
            r["result_state"],
            r["run_duration_s"],
        )
        for r in history
    ]
    round_rows = [
        (
            datetime.fromisoformat(r["started"]),
            r["scenario"],
            r["category"],
            r["clause"],
            r["diagnosis_s"],
            r["applied"],
            r["outcome"],
            r["verify_s"],
        )
        for r in rounds
    ]
    for name, rows, ddl in (("evidence_runs", runs, RUNS_DDL), ("evidence_rounds", round_rows, ROUNDS_DDL)):
        table = f"{schema}.{name}"
        spark.createDataFrame(rows, ddl).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        LOG.info("published", extra={"fields": {"target": table, "rows": len(rows)}})
