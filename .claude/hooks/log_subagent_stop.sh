#!/bin/bash
# SubagentStop hook: appends one evidence/run_journal.jsonl entry per
# completion of this project's diagnostic agents (see CLAUDE.md section 4).
# Scoped via the SubagentStop matcher in settings.json to
# log-parser|classifier|patch-proposer, but that is not trusted alone: this
# script independently hard-validates agent_type against the same three
# exact names before writing anything, so a matcher misconfiguration or an
# unexpected payload can never append an untrusted/bogus entry to evidence.
set -uo pipefail

JOURNAL="${CLAUDE_PROJECT_DIR}/evidence/run_journal.jsonl"
TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PAYLOAD="$(cat)"

AGENT_NAME="$(printf '%s' "$PAYLOAD" | jq -r '.agent_type // empty')"
case "$AGENT_NAME" in
  log-parser|classifier|patch-proposer) ;;
  *) exit 0 ;;
esac

printf '%s' "$PAYLOAD" | jq -c --arg ts "$TIMESTAMP" '
  (.last_assistant_message // "") as $raw |
  {
    timestamp: $ts,
    hook_event: "SubagentStop",
    agent_name: .agent_type,
    session_id: .session_id,
    output: $raw[0:5000],
    truncated: (($raw | length) > 5000)
  }
' >> "$JOURNAL"
