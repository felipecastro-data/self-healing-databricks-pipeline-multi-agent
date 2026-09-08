# CLAUDE.md

## 1. Project Overview

This is a portfolio project demonstrating a self-healing data pipeline for Databricks, orchestrated by a multi-agent diagnostic system. When a Databricks job fails, a deterministic poller detects the failure and hands the raw run output to a chain of specialized agents: one extracts the relevant log signal from the noise, one classifies the failure against a fixed taxonomy of known error categories, and one proposes a templated remediation using specifics pulled from the log (table names, column names, etc.). The goal is to show a constrained, auditable diagnosis-and-remediation loop — not a free-form autonomous fixer — with every step logged as evidence and every applied patch gated behind an approval step.

## 2. Failure Taxonomy

| Category | Log Signature to Detect | Template Remediation |
|---|---|---|
| `schema_drift` | `AnalysisException` / column not found / type mismatch on read | Add explicit schema or `mergeSchema` option |
| `null_violation` | NOT NULL constraint violated / null in non-nullable write | `.na.drop()` or `.fillna()` on the offending column |
| `oom` | `OutOfMemoryError` / executor lost / spill warnings | Repartition or convert join to broadcast |
| `bad_join` | Row explosion / duplicate keys / Cartesian product | Validate join key uniqueness, switch join type |
| `type_mismatch` | Cannot resolve / implicit cast error | Explicit `.cast()` on offending column |

## 3. Agent Responsibilities

**log-parser** — Takes the raw Databricks job run output (stdout/stderr, driver logs, stack traces) and extracts only the lines relevant to the failure: the exception type, the stack trace frames pointing into pipeline code, and any surrounding context (table/column names, partition info) needed to diagnose it. It does not interpret or classify the error — it only reduces noise so downstream agents work from a small, relevant excerpt instead of a full log dump.

**classifier** — Takes the parsed log excerpt and maps it to exactly one of the five categories in the failure taxonomy above, using only the log signatures defined there. It does not perform free-form diagnosis or invent new failure categories — if the excerpt doesn't clearly match a known signature, it reports "unclassified" rather than guessing.

**patch-proposer** — Takes the classified category and the parsed log excerpt, and fills in that category's template remediation with the specific identifiers found in the log (table name, column name, join key, etc.). It does not generate arbitrary code changes or attempt remediations outside the fixed template set — its output is always a filled-in instance of one of the five templates above.

## 4. Architecture Note

`orchestrate.py` is the deterministic trigger for this system: it polls the Databricks Jobs API via the Databricks SDK on a fixed interval, detects failed runs, and kicks off the agent chain (log-parser → classifier → patch-proposer). It is a plain Python script, not a Claude Code hook — the polling loop and failure detection logic run independently of Claude Code's hook system.

Claude Code hooks are used narrowly, for two things only:
- **PreToolUse**: an approval gate that blocks `apply_patch` and `run_job` tool calls until explicitly approved, so no remediation is applied or job re-triggered without a human (or explicit policy) in the loop.
- **PostToolUse` / `SubagentStop`**: logging — every tool call result and subagent completion is appended to `evidence/run_journal.jsonl` so the full diagnostic trail is auditable after the fact.

No other hook logic exists in this project; hooks are not used to drive orchestration.

## 5. Conventions

- **Python style**: standard library + `pyspark`/`pandas`/`databricks-sdk` only for now; PEP 8, type hints on function signatures, docstrings on modules and public functions.
- **Commit messages**: Conventional Commits style (`feat:`, `fix:`, `docs:`, `chore:`, `scaffold:`), imperative mood, one logical change per commit.
- **Evidence**: every diagnostic run's structured trail goes to `evidence/run_journal.jsonl` (one JSON object per line); raw/supporting logs go to `evidence/logs/`. Screenshots documenting the system in action go to `docs/screenshots/`.
