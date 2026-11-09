# ADR 0001: Human approval before any patch or re-run

**Status:** Accepted

## Context

The agents propose a code change for a failed job. Applying it commits to Git, pushes, and syncs the workspace. Re-running a job costs compute and can overwrite tables.

A patch that applies cleanly isn't necessarily a fix. The first `schema_drift` patch (commit `adc19f7`) replaced a missing-column reference with an `assert`. It committed and pushed without error, but the job still failed, only with a different exception. That patch landed before any gate existed (the gate came two days later, in `5c7ddf5`), and it's the reason the gate was added.

## Options

| Option | For | Against |
|---|---|---|
| Apply automatically, re-run, and loop until green | Fastest, no person needed | "Green" can mean the check was removed. A wrong patch reaches the repo unreviewed. |
| Apply automatically only for "safe" categories | Less review work | Needs a definition of safe. The wrong `type_mismatch` and `schema_drift` patches were both inside their templates. |
| Ask a person before every apply and re-run | A person sees every change first | Slower, and doesn't work unattended |

## Decision

Every `apply_patch` and `run_job` call stops at a Claude Code `PreToolUse` hook that asks for approval. The headless session started by `orchestrate.py` has no one to ask, so those two tools are also blocked outright with `--disallowedTools`.

## Consequences

- The system diagnoses unattended but never changes anything unattended.
- Time to fix includes waiting for a person, so it's reported separately from diagnosis time.
- The approval is only as good as the review. It shows the file or job name, so the reviewer has to read the proposed patch itself.
- The gate lives in Claude Code. Someone calling the Databricks API or Git directly isn't stopped by it.
