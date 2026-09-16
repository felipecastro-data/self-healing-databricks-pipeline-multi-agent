#!/bin/bash
# PreToolUse hook: approval gate for apply_patch and run_job (see CLAUDE.md
# section 4). Reads the pending tool call from stdin and asks the user to
# explicitly approve it before it proceeds, printing the tool name and its
# key argument (file_path for apply_patch, job_name for run_job).
set -uo pipefail

jq -c '
  .tool_name as $tool |
  (if ($tool | endswith("apply_patch")) then
     "apply_patch(file_path=" + (.tool_input.file_path // "?") + ")"
   elif ($tool | endswith("run_job")) then
     "run_job(job_name=" + (.tool_input.job_name // "?") + ")"
   else
     $tool
   end) as $desc |
  {
    hookSpecificOutput: {
      hookEventName: "PreToolUse",
      permissionDecision: "ask",
      permissionDecisionReason: ("Pending action: " + $desc + " — requires explicit human approval before it runs (self-healing-pipeline approval gate, CLAUDE.md section 4).")
    }
  }
'
