"""The approval gate, the journal redaction hooks, and the headless trigger's tool block."""

import json
import logging
import subprocess
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from self_healing import orchestrate
from self_healing.jobs.json_log import JsonFormatter

REPO_ROOT = Path(__file__).resolve().parent.parent
HOOKS = REPO_ROOT / ".claude" / "hooks"


def _hook(name: str, payload: dict, project_dir: Path) -> str:
    done = subprocess.run(
        ["bash", str(HOOKS / name)],
        input=json.dumps(payload),
        capture_output=True,
        text=True,
        env={"CLAUDE_PROJECT_DIR": str(project_dir), "PATH": "/usr/bin:/bin:/opt/homebrew/bin"},
        check=True,
    )
    return done.stdout


@pytest.mark.parametrize(
    "tool, argument, expected",
    [
        ("apply_patch", {"file_path": "a.py"}, "apply_patch(file_path=a.py)"),
        ("run_job", {"job_name": "j"}, "run_job(job_name=j)"),
    ],
)
def test_approval_gate_always_asks(tmp_path, tool, argument, expected):
    payload = {"tool_name": f"mcp__databricks-self-healing-pipeline__{tool}", "tool_input": argument}
    decision = json.loads(_hook("approval_gate.sh", payload, tmp_path))["hookSpecificOutput"]
    assert decision["permissionDecision"] == "ask"
    assert expected in decision["permissionDecisionReason"]


@pytest.mark.parametrize(
    "hook, payload",
    [
        ("log_tool_use.sh", {"tool_name": "t", "session_id": "s", "tool_response": {"text": "{TEXT}"}}),
        ("log_subagent_stop.sh", {"agent_type": "classifier", "session_id": "s", "last_assistant_message": "{TEXT}"}),
    ],
)
def test_hooks_redact_workspace_identifiers(tmp_path, hook, payload):
    (tmp_path / "evidence").mkdir()
    text = "https://dbc-1.cloud.databricks.com/jobs /Workspace/Users/someone@example.com/repo"
    filled = json.loads(json.dumps(payload).replace("{TEXT}", text))
    _hook(hook, filled, tmp_path)
    entry = (tmp_path / "evidence" / "run_journal.jsonl").read_text()
    assert "<workspace-host>" in entry and "/Workspace/Users/<user>/repo" in entry
    assert "someone@example.com" not in entry and "dbc-1" not in entry


def test_subagent_hook_reads_handback_from_transcript(tmp_path):
    (tmp_path / "evidence").mkdir()
    transcript = tmp_path / "agent.jsonl"
    content = [{"type": "tool_use", "name": "SubagentHandback", "input": {"report": "Classification: oom"}}]
    transcript.write_text(json.dumps({"type": "assistant", "message": {"content": content}}) + "\n")
    payload = {"agent_type": "classifier", "session_id": "s", "agent_transcript_path": str(transcript)}
    _hook("log_subagent_stop.sh", payload, tmp_path)
    entry = json.loads((tmp_path / "evidence" / "run_journal.jsonl").read_text())
    assert entry["output"] == "Classification: oom"


def _run_main(monkeypatch, state):
    result = {"state": state, "run_id": 1, "run_page_url": "u"}
    monkeypatch.setattr(orchestrate.databricks_mcp, "run_job", lambda name: result)
    monkeypatch.setattr(orchestrate.databricks_mcp, "get_run_output", lambda run_id: "boom")
    monkeypatch.setattr("sys.argv", ["orchestrate", "job"])
    claude = MagicMock(return_value=MagicMock(returncode=0))
    monkeypatch.setattr(orchestrate.subprocess, "run", claude)
    assert orchestrate.main() == 0
    return claude


def test_successful_run_starts_no_diagnosis(monkeypatch):
    _run_main(monkeypatch, "SUCCESS").assert_not_called()


def test_diagnosis_session_cannot_apply_or_rerun(monkeypatch):
    command = _run_main(monkeypatch, "FAILED").call_args.args[0]
    blocked = command[command.index("--disallowedTools") + 1]
    assert "apply_patch" in blocked and "run_job" in blocked


def test_json_log_line_carries_fields():
    record = logging.LogRecord("self_healing.jobs.bad_join", logging.WARNING, "f", 1, "counts", None, None)
    record.fields = {"rows": 3}
    line = json.loads(JsonFormatter().format(record))
    assert (line["level"], line["job"], line["event"], line["rows"]) == ("WARNING", "bad_join", "counts", 3)
