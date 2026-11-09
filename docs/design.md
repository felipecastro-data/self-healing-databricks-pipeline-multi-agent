# Design notes

What the system does today in terms of scale, cost, governance and security, and what would have to change for wider use. Decisions are recorded in the [ADRs](adr/).

## Scalability

**Today:** one job at a time. `orchestrate.py` runs a job in the foreground, waits up to 15 minutes, and diagnoses it if it failed.

| Pressure | What happens now | What would change |
|---|---|---|
| Several failures at once | Each needs its own process. Hooks append to one journal, and the report groups rows in time order, so overlapping rounds would mix. | Group rounds by `session_id` and queue failures with a concurrency limit. |
| Continuous detection | The trigger is run by hand for a named job. | Poll the Jobs API on an interval, or use a job failure webhook, and record which runs were already handled. |
| More failure categories | A category lives in four places: `CLAUDE.md`, `classifier.md`, `patch-proposer.md`, and `CATEGORIES` in `journal.py`. | Generate the agent tables from one taxonomy file. |
| Long logs | The Databricks API can truncate output (the tool marks it). Journal entries keep the first 5,000 characters and set `truncated`. | Store full logs by run id and keep a pointer in the journal. |
| Larger tables | Contract validation scans the whole table once per check, including an exact distinct count of the key. The whole golden job, with both of its validations, took 39 s at 7.5 million rows; validation wasn't timed on its own. | Use approximate counts or validate only new partitions. |

## Cost

**Claude:** one diagnostic round is three short agent calls. Only log-parser sees the full log; the other two work from its excerpt. Token totals measured on 2026-11-09, including each agent's system prompt:

| Agent | Tokens per call |
|---|---:|
| log-parser | 9,097 to 9,501 |
| classifier | 7,184 to 7,596 |
| patch-proposer | 10,032 to 12,317 |
| **One round** | **about 27,000 to 29,000** |

These come from four calls per agent on two logs of about 3,300 characters. Longer logs raise the log-parser figure, and retries of a wrong patch repeat the whole round. The figures are total tokens as reported per subagent. The split between input, output and cached tokens wasn't recorded, so there's no dollar figure here.

**Databricks:** jobs run on Free Edition serverless compute. `self-healing-oom` is unbounded on purpose, so its job has a 15-minute timeout.

**People:** the largest cost is review time. Time to verify includes it, so it's much longer than diagnosis time.

## Governance

- **Audit trail:** hooks append every MCP tool result and agent completion to `evidence/run_journal.jsonl`. Nothing rewrites it automatically, but it's a plain file in Git, so its integrity depends on Git history and review.
- **Retention:** the journal is kept indefinitely in the repo. Databricks keeps runs for 60 days, so durations are saved to `evidence/run_history.json`. A shared deployment would need a written retention period and a store outside the repo.
- **Who may approve:** anyone running Claude Code in the repo. The gate asks for a decision but doesn't check who answers. Wider use needs named approvers, for example a required pull request review with CODEOWNERS.
- **Data contracts:** `contracts/` is versioned with the code, so a contract change is reviewed like any other. The `version` field is informational and nothing enforces compatibility between versions.
- **Limited remedies:** the patch-proposer can only fill one of five templates, and unknown failures are reported as `unclassified`.

## Security

| Area | Today | Risk | Better |
|---|---|---|---|
| Databricks identity | A personal access token in `.env` | It carries all of the user's permissions | A service principal with OAuth and access to only these jobs and schemas, where the workspace allows it |
| MCP tools | Four tools; two are gated | `apply_patch` can edit any file in the repo, including hooks and CI | Limit it to `src/self_healing/jobs/` |
| Git pushes | The whole current branch ([ADR 0003](adr/0003-push-whole-branch.md)) | Other local commits are published too | One branch and pull request per patch |
| Log content | Passed to the agents as text | A log line could contain instructions aimed at a model | Agents have read-only tools, and every change still needs approval |
| Secrets in evidence | Hooks redact the workspace host and user path | Only those two patterns are covered | Add secret scanning in CI |
| Git history | Current files are redacted | Earlier commits still contain the workspace host and user path | Rewrite history or rotate to a new workspace if this matters |
