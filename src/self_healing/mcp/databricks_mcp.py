"""MCP server exposing Databricks Jobs API operations to agents.

Built on the official Python MCP SDK (FastMCP). Exposes five tools that
replace the manual Databricks-UI copy/paste steps in this project's
diagnose-and-remediate loop:

- list_runs        — recent run history for a named job
- get_run_output    — plain-text error/output for one run (feeds log-parser)
- get_table_lineage — read-only: the job's input tables and where they come from
- apply_patch       — applies patch-proposer's diff, commits, pushes, and
                       syncs the workspace Git folder
- run_job           — triggers a job and waits for it to finish

All tools authenticate via DATABRICKS_HOST / DATABRICKS_TOKEN loaded from
.env; get_table_lineage also needs DATABRICKS_WAREHOUSE_ID. job_name -> job_id
resolution looks the job up by its exact name in the workspace; the jobs
themselves are declared in databricks.yml.

Run standalone for local testing: python -m self_healing.mcp.databricks_mcp
"""

from __future__ import annotations

import difflib
import os
import re
import subprocess
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service.jobs import BaseRun, RunLifeCycleState
from databricks.sdk.service.sql import StatementParameterListItem, StatementState
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parents[3]

# Strips terminal color codes out of Databricks error tracebacks so the text
# handed to the log-parser agent is clean, not a wall of \x1b[...m sequences.
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")

# run_now_and_wait's SDK default is 20 minutes, which is an implicit choice
# buried in a library default rather than one made for this project. Bounding
# it explicitly here instead: every job in this project normally finishes in
# well under a minute, and the worst observed case (oom.py, deliberately
# pathological) took ~13 minutes. 15 minutes gives that worst case headroom
# without leaving orchestration able to hang indefinitely on a stuck run.
RUN_JOB_TIMEOUT = timedelta(minutes=15)

# Covers a stopped serverless warehouse starting up before the lineage query runs.
SQL_TIMEOUT = timedelta(minutes=3)

mcp = FastMCP("databricks-self-healing-pipeline")


def _client() -> WorkspaceClient:
    """Build a WorkspaceClient from DATABRICKS_HOST/DATABRICKS_TOKEN in .env."""
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    return WorkspaceClient(host=host, token=token)


def _git_folder() -> str:
    """Return the workspace Git folder path from DATABRICKS_GIT_FOLDER in .env."""
    git_folder = os.environ.get("DATABRICKS_GIT_FOLDER", "").rstrip("/")
    if not git_folder:
        raise RuntimeError("DATABRICKS_GIT_FOLDER is not set. Add it to .env (see .env.example).")
    return git_folder


def _resolve_job_id(w: WorkspaceClient, job_name: str) -> int:
    """Return the id of the one job with exactly this name; refuse zero or several."""
    matches = [j.job_id for j in w.jobs.list(name=job_name) if j.settings and j.settings.name == job_name]
    if len(matches) != 1:
        raise ValueError(
            f"Expected exactly one job named '{job_name}', found {len(matches)}. "
            "Deploy the bundle first (make setup) or check the job name."
        )
    return matches[0]


def _run_state_label(run: BaseRun) -> str:
    """Collapse a run's (possibly-absent) state/status into one short string."""
    if run.state and run.state.result_state:
        return run.state.result_state.value
    if run.state and run.state.life_cycle_state:
        return run.state.life_cycle_state.value
    if run.status and run.status.termination_details and run.status.termination_details.code:
        return run.status.termination_details.code.value
    if run.status and run.status.state:
        return run.status.state.value
    return "UNKNOWN"


def _epoch_ms_to_iso(ms: int | None) -> str | None:
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=UTC).isoformat()


def _run_git(*args: str) -> str:
    """Run a git command in the repo root; raise loudly on any non-zero exit."""
    result = subprocess.run(
        ["git", *args],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"git {' '.join(args)} failed (exit {result.returncode}):\n{result.stderr.strip()}"
        )
    return result.stdout.strip()


@mcp.tool()
def list_runs(job_name: str, limit: int = 5) -> list[dict[str, Any]]:
    """List recent run summaries for a named Databricks job.

    job_name is resolved to a job_id by its exact name in the workspace.

    Returns each run's task-level run_id — not the parent job-level run_id —
    because get_run_output only accepts task-level run_ids (the Jobs API
    rejects get_run_output on a run with multiple tasks, and every job in
    this project runs a single spark_python_task per run). Feed this run_id
    straight into get_run_output.
    """
    w = _client()
    job_id = _resolve_job_id(w, job_name)
    runs = list(w.jobs.list_runs(job_id=job_id, limit=limit, expand_tasks=True))

    summaries = []
    for r in runs:
        task_run_id = r.tasks[-1].run_id if r.tasks else r.run_id
        summaries.append(
            {
                "run_id": task_run_id,
                "job_run_id": r.run_id,
                "state": _run_state_label(r),
                "start_time": _epoch_ms_to_iso(r.start_time),
                "run_page_url": r.run_page_url,
            }
        )
    return summaries


@mcp.tool()
def get_run_output(run_id: str) -> str:
    """Fetch the error/output text for one Databricks task run, as plain text.

    This is the tool that feeds the log-parser agent, replacing manual
    copy-paste from the Databricks UI. run_id must be a task-level run_id
    (as returned by list_runs), not a parent job-level run_id.
    """
    try:
        rid = int(run_id)
    except ValueError as exc:
        raise ValueError(f"run_id must be numeric, got {run_id!r}") from exc

    w = _client()
    out = w.jobs.get_run_output(run_id=rid)

    # stdout (print() output) is a separate field from the exception — a
    # script can print diagnostic values (row counts, computed ratios, etc.)
    # that never make it into the exception message itself, so this must be
    # captured too, not just error/error_trace.
    parts = []
    if out.logs:
        logs = out.logs.rstrip("\n")
        if out.logs_truncated:
            logs += "\n(...logs truncated by the Databricks API...)"
        parts.append(logs)
    if out.error:
        parts.append(out.error)
    if out.error_trace:
        parts.append(out.error_trace)
    text = "\n".join(parts)
    text = _ANSI_RE.sub("", text)

    # The run's terminal state_message (e.g. "Workload failed, see run output
    # for details") is what a human sees on the run page alongside the
    # traceback — append it if present and not already folded into the text.
    try:
        run = w.jobs.get_run(run_id=rid)
        msg = run.state.state_message if run.state else None
        if msg and msg not in text:
            text = f"{text}\n{msg}" if text else msg
    except Exception:
        pass  # state_message is supplementary; don't fail the call over it.

    if not text:
        text = (
            "(no error/output text for this run — it may have succeeded, or its "
            "output is a notebook/SQL/dashboard result type this tool doesn't parse)"
        )
    return text


def _warehouse_id() -> str:
    """Return the SQL warehouse id from DATABRICKS_WAREHOUSE_ID in .env."""
    warehouse_id = os.environ.get("DATABRICKS_WAREHOUSE_ID", "").strip()
    if not warehouse_id:
        raise RuntimeError("DATABRICKS_WAREHOUSE_ID is not set. Add it to .env (see .env.example).")
    return warehouse_id


def _run_sql(w: WorkspaceClient, statement: str, params: list[StatementParameterListItem]) -> list[list[str | None]]:
    """Run one parameterized SQL statement on the warehouse and return its rows; raise on failure."""
    resp = w.statement_execution.execute_statement(
        warehouse_id=_warehouse_id(),
        statement=statement,
        parameters=params,
        wait_timeout="50s",
    )
    deadline = time.monotonic() + SQL_TIMEOUT.total_seconds()
    while resp.status.state in (StatementState.PENDING, StatementState.RUNNING):
        if time.monotonic() > deadline:
            w.statement_execution.cancel_execution(resp.statement_id)
            raise TimeoutError(f"SQL statement did not finish within {SQL_TIMEOUT}")
        time.sleep(2)
        resp = w.statement_execution.get_statement(resp.statement_id)
    if resp.status.state != StatementState.SUCCEEDED:
        error = resp.status.error.message if resp.status.error else resp.status.state.value
        raise RuntimeError(f"SQL statement failed: {error}")
    return (resp.result.data_array if resp.result else None) or []


# The tables a job reads, and who last wrote each of them from what. In this
# project a job's target is built from the tables it reads, so these are the
# target's upstream; a failed run still records its reads.
_UPSTREAM_SQL = """
WITH inputs AS (
  SELECT DISTINCT source_table_full_name AS input_table
  FROM system.access.table_lineage
  WHERE entity_type = 'JOB' AND entity_id = :job_id
    AND source_table_full_name IS NOT NULL
    AND event_date >= date_sub(current_date(), :days)
)
SELECT i.input_table, l.source_table_full_name, l.entity_id, CAST(max(l.event_time) AS STRING)
FROM inputs i
LEFT JOIN system.access.table_lineage l
  ON l.target_table_full_name = i.input_table
  AND l.source_table_full_name IS NOT NULL
  AND l.entity_type = 'JOB'
  AND l.event_date >= date_sub(current_date(), :days)
GROUP BY ALL
ORDER BY 1, 4 DESC
"""


@mcp.tool()
def get_table_lineage(job_name: str, days: int = 90) -> list[dict[str, Any]]:
    """List the tables a named job reads and where each one comes from, from system.access.table_lineage.

    Read-only. Each row is one input table of the job (the upstream of its
    target), the table that input was last built from, the job that built it,
    and when. A contract_drift diagnosis uses this to name the source that
    changed. Upstream fields are None when no job wrote the input in the window.
    """
    if not 1 <= days <= 365:
        raise ValueError(f"days must be between 1 and 365, got {days}")
    w = _client()
    job_id = _resolve_job_id(w, job_name)
    params = [
        StatementParameterListItem(name="job_id", value=str(job_id)),
        StatementParameterListItem(name="days", value=str(days), type="INT"),
    ]
    rows = _run_sql(w, _UPSTREAM_SQL, params)

    names: dict[str, str] = {}
    for _, _, producer_id, _ in rows:
        if producer_id and producer_id not in names:
            try:
                names[producer_id] = w.jobs.get(job_id=int(producer_id)).settings.name
            except Exception:
                names[producer_id] = producer_id  # deleted or not visible: keep the id
    return [
        {
            "input_table": input_table,
            "upstream_table": upstream,
            "upstream_job": names.get(producer_id) if producer_id else None,
            "last_written": last_written,
        }
        for input_table, upstream, producer_id, last_written in rows
    ]


@mcp.tool()
def apply_patch(
    file_path: str,
    old_snippet: str,
    new_snippet: str,
    category: str | None = None,
) -> dict[str, Any]:
    """Apply patch-proposer's diff-style output to a file, commit, push, and sync the workspace.

    old_snippet must match the file's current contents exactly once. Zero
    matches or more than one match is a loud failure — no fuzzy matching, no
    silent partial replacement.

    On success: stages and commits the change ("fix: apply {category} patch
    to {file_path}", or "fix: apply patch to {file_path}" if category is
    omitted), pushes to origin, then calls the Databricks Repos API to pull
    the new commit into the workspace Git folder at DATABRICKS_GIT_FOLDER so the
    workspace copy is synced without a manual UI step.

    The commit and push are not rolled back on any failure below them, so a
    partial-failure state is always visible rather than silently discarded:
    if the git commit or push fails, the file edit is left in place,
    uncommitted or unpushed, for a human to inspect. If the push succeeds but
    the Databricks-side sync doesn't land — the API call errors, or the
    workspace Git folder's head commit doesn't match what was just pushed —
    this raises loudly (RuntimeError) rather than returning a
    "sync worked/didn't" field for the caller to remember to check. A
    successful return always means the workspace copy is confirmed in sync.
    """
    git_folder = _git_folder()
    target = (REPO_ROOT / file_path).resolve()
    if REPO_ROOT not in target.parents:
        raise ValueError(f"{file_path} resolves outside the repo root — refusing to touch it")
    if not target.is_file():
        raise FileNotFoundError(f"{file_path} does not exist under repo root {REPO_ROOT}")

    content = target.read_text()
    match_count = content.count(old_snippet)
    if match_count == 0:
        raise ValueError(f"old_snippet not found in {file_path} (0 matches) — refusing to guess")
    if match_count > 1:
        raise ValueError(
            f"old_snippet matches {match_count} times in {file_path} — need exactly 1 "
            "unambiguous match, refusing a fuzzy/partial replacement"
        )

    new_content = content.replace(old_snippet, new_snippet, 1)
    target.write_text(new_content)

    diff_text = "".join(
        difflib.unified_diff(
            content.splitlines(keepends=True),
            new_content.splitlines(keepends=True),
            fromfile=f"a/{file_path}",
            tofile=f"b/{file_path}",
        )
    )

    commit_message = (
        f"fix: apply {category} patch to {file_path}" if category else f"fix: apply patch to {file_path}"
    )

    _run_git("add", file_path)
    _run_git("commit", "-m", commit_message)
    commit_hash = _run_git("rev-parse", "HEAD")
    branch = _run_git("rev-parse", "--abbrev-ref", "HEAD")
    _run_git("push", "origin", branch)

    w = _client()
    repo_status = w.workspace.get_status(git_folder)
    repo_id = repo_status.object_id
    w.repos.update(repo_id=repo_id, branch=branch)
    synced_repo = w.repos.get(repo_id=repo_id)

    if synced_repo.head_commit_id != commit_hash:
        raise RuntimeError(
            f"Workspace Git folder sync did not take: pushed {commit_hash} to origin/{branch}, "
            f"but {git_folder} (repo_id={repo_id}) is now at {synced_repo.head_commit_id} instead. "
            "The commit and push already succeeded — this failure is the sync step only. "
            "Re-run the Databricks Repos update (or apply_patch's sync step) before triggering "
            "run_job, or the job will execute against stale workspace code."
        )

    return {
        "file_path": file_path,
        "diff": diff_text,
        "commit_hash": commit_hash,
        "commit_message": commit_message,
        "pushed_to": f"origin/{branch}",
        "workspace_sync": {
            "git_folder": git_folder,
            "synced_head_commit_id": synced_repo.head_commit_id,
            "matches_pushed_commit": True,
        },
    }


@mcp.tool()
def run_job(job_name: str) -> dict[str, Any]:
    """Trigger a Databricks job and wait for it to reach a terminal state.

    job_name is resolved to a job_id by its exact name. Waiting is bounded by
    RUN_JOB_TIMEOUT (15 minutes — see its definition for reasoning); if the
    run hasn't reached a terminal state by then, the SDK's waiter raises
    TimeoutError rather than this tool hanging indefinitely.

    A single-task job's parent run reports a task failure via life-cycle
    state INTERNAL_ERROR rather than TERMINATED — expected and common for
    the broken-scenario jobs in this project, which are designed to fail.
    The SDK's waiter (traced against databricks-sdk's
    wait_get_run_job_terminated_or_skipped) raises OperationFailed
    exclusively for that INTERNAL_ERROR case — timeouts raise TimeoutError
    instead, and a transient API error while polling propagates as its own
    native exception type, neither of which land here. Even so, this does
    not just trust that OperationFailed means "safe to report FAILED": it
    re-fetches the exact run we triggered (via the run_id the SDK hands back
    immediately from run_now, not a guess from run history) and only treats
    it as a normal result if that run's own state confirms
    INTERNAL_ERROR with a resolved result_state. Anything else re-raises the
    original OperationFailed rather than silently returning a clean-looking
    result for a wait failure that wasn't actually a task failure.

    Like list_runs, the returned run_id is the task-level run_id, not the
    parent job-level run_id — get_run_output only accepts the former (the
    Jobs API rejects it on a multi-task parent run), so this must resolve it
    the same way list_runs does rather than handing back the raw run_id
    run_now/get_run report at the job level. Feed this run_id straight into
    get_run_output.
    """
    w = _client()
    job_id = _resolve_job_id(w, job_name)
    waiter = w.jobs.run_now(job_id=job_id)
    run_id = waiter.run_id
    try:
        run: BaseRun = waiter.result(timeout=RUN_JOB_TIMEOUT)
    except OperationFailed:
        run = w.jobs.get_run(run_id=run_id)
        life_cycle_state = run.state.life_cycle_state if run.state else None
        result_state = run.state.result_state if run.state else None
        if life_cycle_state != RunLifeCycleState.INTERNAL_ERROR or result_state is None:
            raise
    task_run_id = run.tasks[-1].run_id if run.tasks else run.run_id
    return {
        "job_name": job_name,
        "run_id": task_run_id,
        "job_run_id": run.run_id,
        "state": _run_state_label(run),
        "run_page_url": run.run_page_url,
    }


if __name__ == "__main__":
    mcp.run()
