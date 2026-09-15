"""MCP server exposing Databricks Jobs API operations to agents.

Built on the official Python MCP SDK (FastMCP). Exposes four tools that
replace the manual Databricks-UI copy/paste steps in this project's
diagnose-and-remediate loop:

- list_runs        — recent run history for a named job
- get_run_output    — plain-text error/output for one run (feeds log-parser)
- apply_patch       — applies patch-proposer's diff, commits, pushes, and
                       syncs the workspace Git folder
- run_job           — triggers a job and waits for it to finish

All tools authenticate via DATABRICKS_HOST / DATABRICKS_TOKEN loaded from
.env, the same pattern used in scripts/setup_databricks_jobs.py. job_name ->
job_id resolution uses job_ids.json in the repo root, produced by that same
script.

Run standalone for local testing: python mcp_server/databricks_mcp.py
"""

from __future__ import annotations

import difflib
import json
import os
import re
import subprocess
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service.jobs import BaseRun, RunLifeCycleState
from dotenv import load_dotenv
from mcp.server.fastmcp import FastMCP

load_dotenv()

REPO_ROOT = Path(__file__).resolve().parent.parent
JOB_IDS_FILE = REPO_ROOT / "job_ids.json"
GIT_FOLDER = "/Workspace/Users/felipe9393@icloud.com/self-healing-databricks-pipeline-multi-agent"

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

mcp = FastMCP("databricks-self-healing-pipeline")


def _client() -> WorkspaceClient:
    """Build a WorkspaceClient from DATABRICKS_HOST/DATABRICKS_TOKEN in .env."""
    host = os.environ["DATABRICKS_HOST"].rstrip("/")
    token = os.environ["DATABRICKS_TOKEN"]
    return WorkspaceClient(host=host, token=token)


def _load_job_ids() -> dict[str, int]:
    if not JOB_IDS_FILE.exists():
        raise FileNotFoundError(
            f"{JOB_IDS_FILE} does not exist — run scripts/setup_databricks_jobs.py first "
            "to register the pipeline jobs and generate this file."
        )
    return json.loads(JOB_IDS_FILE.read_text())


def _resolve_job_id(job_name: str) -> int:
    job_ids = _load_job_ids()
    if job_name not in job_ids:
        known = ", ".join(sorted(job_ids))
        raise ValueError(f"Unknown job_name '{job_name}'. Known jobs: {known}")
    return job_ids[job_name]


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


def _epoch_ms_to_iso(ms: Optional[int]) -> Optional[str]:
    if not ms:
        return None
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).isoformat()


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

    job_name is resolved to a job_id via job_ids.json in the repo root (the
    file produced by scripts/setup_databricks_jobs.py).

    Returns each run's task-level run_id — not the parent job-level run_id —
    because get_run_output only accepts task-level run_ids (the Jobs API
    rejects get_run_output on a run with multiple tasks, and every job in
    this project runs a single spark_python_task per run). Feed this run_id
    straight into get_run_output.
    """
    job_id = _resolve_job_id(job_name)
    w = _client()
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


@mcp.tool()
def apply_patch(
    file_path: str,
    old_snippet: str,
    new_snippet: str,
    category: Optional[str] = None,
) -> dict[str, Any]:
    """Apply patch-proposer's diff-style output to a file, commit, push, and sync the workspace.

    old_snippet must match the file's current contents exactly once. Zero
    matches or more than one match is a loud failure — no fuzzy matching, no
    silent partial replacement (same fail-loud principle as
    orchestrate.parse_classification).

    On success: stages and commits the change ("fix: apply {category} patch
    to {file_path}", or "fix: apply patch to {file_path}" if category is
    omitted), pushes to origin, then calls the Databricks Repos API to pull
    the new commit into the workspace Git folder at GIT_FOLDER so the
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
    repo_status = w.workspace.get_status(GIT_FOLDER)
    repo_id = repo_status.object_id
    w.repos.update(repo_id=repo_id, branch=branch)
    synced_repo = w.repos.get(repo_id=repo_id)

    if synced_repo.head_commit_id != commit_hash:
        raise RuntimeError(
            f"Workspace Git folder sync did not take: pushed {commit_hash} to origin/{branch}, "
            f"but {GIT_FOLDER} (repo_id={repo_id}) is now at {synced_repo.head_commit_id} instead. "
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
            "git_folder": GIT_FOLDER,
            "synced_head_commit_id": synced_repo.head_commit_id,
            "matches_pushed_commit": True,
        },
    }


@mcp.tool()
def run_job(job_name: str) -> dict[str, Any]:
    """Trigger a Databricks job and wait for it to reach a terminal state.

    job_name is resolved to a job_id via job_ids.json. Waiting is bounded by
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
    """
    job_id = _resolve_job_id(job_name)
    w = _client()
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
    return {
        "job_name": job_name,
        "run_id": run.run_id,
        "state": _run_state_label(run),
        "run_page_url": run.run_page_url,
    }


if __name__ == "__main__":
    mcp.run()
