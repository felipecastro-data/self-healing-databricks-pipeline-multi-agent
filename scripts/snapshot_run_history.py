"""Merge recent Databricks run history into evidence/run_history.json.

The Jobs API keeps runs for 60 days, so results are saved here instead of
being read live. Existing entries are never removed.

Usage: python scripts/snapshot_run_history.py
"""

import json
from datetime import UTC, datetime
from pathlib import Path

import yaml

from self_healing.mcp.databricks_mcp import _client

REPO_ROOT = Path(__file__).resolve().parent.parent
HISTORY_FILE = REPO_ROOT / "evidence" / "run_history.json"


def main() -> None:
    """Fetch the latest runs of every dev and prod job in databricks.yml and merge them by run id."""
    bundle = yaml.safe_load((REPO_ROOT / "databricks.yml").read_text())
    runs = {row["job_run_id"]: row for row in json.loads(HISTORY_FILE.read_text())}
    prefix = bundle["targets"]["prod"]["presets"]["name_prefix"]
    names = [job["name"] for job in bundle["resources"]["jobs"].values()]

    w = _client()
    # jobs.list matches the exact name and yields nothing for an undeployed target.
    for name in names + [prefix + name for name in names]:
        for job in w.jobs.list(name=name):
            for run in w.jobs.list_runs(job_id=job.job_id, limit=25):
                if run.state and run.state.result_state:
                    started = datetime.fromtimestamp(run.start_time / 1000, tz=UTC)
                    runs[run.run_id] = {
                        "job_name": name,
                        "job_run_id": run.run_id,
                        "start_time": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
                        "result_state": run.state.result_state.value,
                        "run_duration_s": round((run.run_duration or 0) / 1000),
                    }

    ordered = sorted(runs.values(), key=lambda row: row["start_time"])
    HISTORY_FILE.write_text(json.dumps(ordered, indent=2) + "\n")
    print(f"{len(ordered)} runs in {HISTORY_FILE.name}")


if __name__ == "__main__":
    main()
