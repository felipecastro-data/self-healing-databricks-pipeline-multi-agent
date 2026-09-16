#!/bin/bash
# PostToolUse hook: appends one evidence/run_journal.jsonl entry per
# diagnostic-chain MCP tool call result (see CLAUDE.md section 4). Scoped via
# the PostToolUse matcher to mcp__databricks-self-healing-pipeline__* only,
# so ordinary Read/Edit/Bash tool use elsewhere in this repo isn't logged as
# diagnostic evidence.
set -uo pipefail

JOURNAL="${CLAUDE_PROJECT_DIR}/evidence/run_journal.jsonl"
TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

jq -c --arg ts "$TIMESTAMP" '
  (.tool_response | tostring) as $raw |
  {
    timestamp: $ts,
    hook_event: "PostToolUse",
    tool_name: .tool_name,
    session_id: .session_id,
    output: $raw[0:5000],
    truncated: (($raw | length) > 5000)
  }
' >> "$JOURNAL"
