"""Deterministic trigger for the self-healing pipeline's diagnostic chain (see CLAUDE.md section 4).

Given a job_name, this script triggers that Databricks job via the MCP
server's run_job tool and, if the run fails, fetches its output via
get_run_output, plus the job's table lineage via get_table_lineage, and hands off to a headless Claude Code session
(`claude -p`) that runs the log-parser -> classifier -> patch-proposer
chain defined in .claude/agents/ and CLAUDE.md.

This is a thin trigger only: the diagnostic logic itself lives entirely in
the subagents and CLAUDE.md, not here. It never applies a patch or
re-triggers the job — it prints whatever the diagnostic chain proposes and
stops. Applying the patch (apply_patch) and re-running the job (run_job) are
gated behind the PreToolUse approval hook in .claude/settings.json and stay
manual, human-approved steps performed interactively afterward.

Usage: python -m self_healing.orchestrate <job_name> (or: make demo JOB=<job_name>)
"""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path

from self_healing.mcp import databricks_mcp

REPO_ROOT = Path(__file__).resolve().parents[2]

# Belt-and-suspenders alongside the PreToolUse approval hook in
# .claude/settings.json: the headless session below has no human present to
# answer an approval prompt, so these are blocked outright rather than
# merely gated, regardless of what the hook decides.
_DISALLOWED_TOOLS = (
    "mcp__databricks-self-healing-pipeline__apply_patch,"
    "mcp__databricks-self-healing-pipeline__run_job"
)


def _format_lineage(job_name: str) -> str:
    """Return the job's input tables and their upstream as text lines, or why they are unavailable.

    Lineage is supplementary context, so a failure here never stops the diagnosis.
    """
    try:
        rows = databricks_mcp.get_table_lineage(job_name)
    except Exception as exc:
        return f"unavailable ({type(exc).__name__}: {exc})"
    if not rows:
        return "no lineage recorded for this job"
    return "\n".join(
        f"{r['input_table']} <- {r['upstream_table'] or 'no recorded writer'}"
        f" (job {r['upstream_job'] or 'n/a'}, last written {r['last_written'] or 'n/a'})"
        for r in rows
    )


def _build_diagnostic_prompt(
    job_name: str, run_id: int, run_page_url: str | None, raw_output: str, lineage: str
) -> str:
    """Build the headless-session prompt that chains log-parser -> classifier -> patch-proposer."""
    return f"""A Databricks job run has failed. Run this project's diagnostic chain (see
CLAUDE.md section 3) on the run output below, then stop.

Job: {job_name}
Run ID: {run_id}
Run page: {run_page_url or "unavailable"}

Raw run output:
---
{raw_output}
---

Table lineage (system.access.table_lineage: the job's input tables <- what they were built from):
---
{lineage}
---

Steps:
1. Invoke the `log-parser` subagent with the raw run output and the table lineage above as its input.
   Show its structured excerpt.
2. Invoke the `classifier` subagent with log-parser's excerpt as its input. Show its classification.
3. Invoke the `patch-proposer` subagent with classifier's output and log-parser's excerpt as its input.
   Show its proposed patch.

Do not call apply_patch or run_job, and do not re-trigger the job. Stop after
printing patch-proposer's output — applying the patch and re-running the job
are manual, human-approved steps performed afterward, not part of this run.
"""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "job_name",
        help="Databricks job name declared in databricks.yml",
    )
    args = parser.parse_args()

    print(f"Triggering job '{args.job_name}'...", flush=True)
    result = databricks_mcp.run_job(args.job_name)
    print(
        f"Run finished: state={result['state']} run_id={result['run_id']} url={result['run_page_url']}",
        flush=True,
    )

    if result["state"] != "FAILED":
        print(f"Run did not fail (state={result['state']}) — nothing to diagnose.", flush=True)
        return 0

    print("Fetching run output...", flush=True)
    raw_output = databricks_mcp.get_run_output(str(result["run_id"]))

    lineage = _format_lineage(args.job_name)
    prompt = _build_diagnostic_prompt(args.job_name, result["run_id"], result["run_page_url"], raw_output, lineage)

    print(
        "Invoking diagnostic chain (log-parser -> classifier -> patch-proposer) via headless Claude Code...\n",
        flush=True,
    )
    completed = subprocess.run(
        [
            "claude",
            "-p",
            prompt,
            "--permission-prompts",
            "none",
            "--disallowedTools",
            _DISALLOWED_TOOLS,
        ],
        cwd=REPO_ROOT,
    )
    return completed.returncode


if __name__ == "__main__":
    sys.exit(main())
