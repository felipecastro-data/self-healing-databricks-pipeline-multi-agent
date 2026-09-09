"""setup_databricks_jobs.py — registers the six pipeline scripts as Databricks Jobs.

Uses databricks-sdk to idempotently create one Databricks Job per pipeline
script (golden_job.py plus the five broken_scenarios/*.py files), each with
a single spark_python_task pointing at the corresponding file inside the Git
folder checked out in the workspace at:

    /Workspace/Users/felipe9393@icloud.com/self-healing-databricks-pipeline-multi-agent

No cluster spec is attached to any task, so each job defaults to serverless
compute. If a job with the target name already exists, creation is skipped
and its existing job_id is reused instead of creating a duplicate. Resulting
job_ids are written to job_ids.json in the repo root (gitignored —
workspace-specific, not something to commit).

Loads DATABRICKS_HOST / DATABRICKS_TOKEN from .env via python-dotenv, same
pattern as test_connection.py.

Run locally: python scripts/setup_databricks_jobs.py
"""

import json
import os
from pathlib import Path
from typing import Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Environment
from databricks.sdk.service.jobs import JobEnvironment, SparkPythonTask, Source, Task
from dotenv import load_dotenv

load_dotenv()

GIT_FOLDER = "/Workspace/Users/felipe9393@icloud.com/self-healing-databricks-pipeline-multi-agent"

JOBS = [
    ("self-healing-golden-job", "pipelines/golden_job.py"),
    ("self-healing-schema-drift", "pipelines/broken_scenarios/schema_drift.py"),
    ("self-healing-null-violation", "pipelines/broken_scenarios/null_violation.py"),
    ("self-healing-oom", "pipelines/broken_scenarios/oom.py"),
    ("self-healing-bad-join", "pipelines/broken_scenarios/bad_join.py"),
    ("self-healing-type-mismatch", "pipelines/broken_scenarios/type_mismatch.py"),
]

REPO_ROOT = Path(__file__).resolve().parent.parent
JOB_IDS_FILE = REPO_ROOT / "job_ids.json"

# A serverless spark_python_task must reference a job-level environment
# (Jobs API 2.2 requires this even for the simplest single-task job).
# environment_version="3" is the current serverless Python environment.
ENVIRONMENT_KEY = "default"
ENVIRONMENT_VERSION = "3"


def find_existing_job_id(w: WorkspaceClient, name: str) -> Optional[int]:
    """Return the job_id of an existing job with this exact name, if any."""
    for job in w.jobs.list(name=name):
        if job.settings and job.settings.name == name:
            return job.job_id
    return None


def create_job(w: WorkspaceClient, name: str, relative_path: str) -> int:
    """Create a single-task job running relative_path via spark_python_task."""
    workspace_path = f"{GIT_FOLDER}/{relative_path}"
    task = Task(
        task_key=name,
        spark_python_task=SparkPythonTask(python_file=workspace_path, source=Source.WORKSPACE),
        environment_key=ENVIRONMENT_KEY,
    )
    environment = JobEnvironment(
        environment_key=ENVIRONMENT_KEY,
        spec=Environment(environment_version=ENVIRONMENT_VERSION),
    )
    response = w.jobs.create(name=name, tasks=[task], environments=[environment])
    return response.job_id


def main() -> None:
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    w = WorkspaceClient(host=host, token=token)

    job_ids: dict[str, int] = {}

    for name, relative_path in JOBS:
        existing_id = find_existing_job_id(w, name)
        if existing_id is not None:
            print(f"[skip] job '{name}' already exists (job_id={existing_id})")
            job_ids[name] = existing_id
            continue

        job_id = create_job(w, name, relative_path)
        print(f"[create] job '{name}' created (job_id={job_id})")
        job_ids[name] = job_id

    JOB_IDS_FILE.write_text(json.dumps(job_ids, indent=2) + "\n")
    print(f"\nWrote job IDs to {JOB_IDS_FILE}")

    print(f"\n{'JOB NAME':<32} {'JOB ID':<12} JOBS UI URL")
    print("-" * 100)
    for name, job_id in job_ids.items():
        url = f"{host}/jobs/{job_id}"
        print(f"{name:<32} {job_id:<12} {url}")


if __name__ == "__main__":
    main()
