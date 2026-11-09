#!/bin/bash
# SubagentStop hook: appends one evidence/run_journal.jsonl entry per
# completion of this project's diagnostic agents (see CLAUDE.md section 4).
# Scoped via the SubagentStop matcher in settings.json to
# log-parser|classifier|patch-proposer, but that is not trusted alone: this
# script independently hard-validates agent_type against the same three
# exact names before writing anything, so a matcher misconfiguration or an
# unexpected payload can never append an untrusted/bogus entry to evidence.
# Workspace host and user path are redacted before anything reaches the journal.
set -uo pipefail

JOURNAL="${CLAUDE_PROJECT_DIR}/evidence/run_journal.jsonl"
TIMESTAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
PAYLOAD="$(cat)"

AGENT_NAME="$(printf '%s' "$PAYLOAD" | jq -r '.agent_type // empty')"
case "$AGENT_NAME" in
  log-parser|classifier|patch-proposer) ;;
  *) exit 0 ;;
esac

# Some clients end a subagent with a hand-back tool call instead of a final
# text message, leaving last_assistant_message empty. The transcript has it,
# but may not be flushed yet when this hook fires, so retry briefly.
MESSAGE="$(printf '%s' "$PAYLOAD" | jq -r '.last_assistant_message // empty')"
TRANSCRIPT="$(printf '%s' "$PAYLOAD" | jq -r '.agent_transcript_path // empty')"
for _ in 1 2 3 4 5 6 7 8 9 10; do
  { [ -n "$MESSAGE" ] || [ ! -f "$TRANSCRIPT" ]; } && break
  MESSAGE="$(jq -rs '
    [.[] | select(.type == "assistant") | .message.content[]?
     | if .type == "tool_use" and (.name | test("Handback")) then (.input | [.. | strings] | join("\n"))
       else empty end]
    | last // ""
  ' "$TRANSCRIPT")"
  [ -n "$MESSAGE" ] || sleep 0.5
done

printf '%s' "$PAYLOAD" | jq -c --arg ts "$TIMESTAMP" --arg message "$MESSAGE" '
  ($message | (. | gsub("https://[A-Za-z0-9.-]+\\.cloud\\.databricks\\.com"; "https://<workspace-host>") | gsub("/Workspace/Users/[^/\"\\\\ ]+"; "/Workspace/Users/<user>"))) as $raw |
  {
    timestamp: $ts,
    hook_event: "SubagentStop",
    agent_name: .agent_type,
    session_id: .session_id,
    output: $raw[0:5000],
    truncated: (($raw | length) > 5000)
  }
' >> "$JOURNAL"
