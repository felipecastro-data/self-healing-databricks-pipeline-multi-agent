"""The run-history snapshot covers prod jobs and tolerates an undeployed target."""

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock


def _load_script():
    path = Path(__file__).resolve().parent.parent / "scripts" / "snapshot_run_history.py"
    spec = importlib.util.spec_from_file_location("snapshot_run_history", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_records_prod_runs_and_skips_jobs_that_are_not_deployed(tmp_path, monkeypatch):
    script = _load_script()
    history = tmp_path / "run_history.json"
    history.write_text("[]")
    monkeypatch.setattr(script, "HISTORY_FILE", history)

    client = MagicMock()
    client.jobs.list.side_effect = lambda name: (
        [SimpleNamespace(job_id=7)] if name == "prod-self-healing-oom" else []
    )
    client.jobs.list_runs.return_value = [
        SimpleNamespace(
            run_id=1,
            start_time=1_000_000,
            run_duration=900_000,
            state=SimpleNamespace(result_state=SimpleNamespace(value="TIMEDOUT")),
        )
    ]
    monkeypatch.setattr(script, "_client", lambda: client)

    script.main()

    rows = json.loads(history.read_text())
    assert [(r["job_name"], r["result_state"], r["run_duration_s"]) for r in rows] == [
        ("prod-self-healing-oom", "TIMEDOUT", 900)
    ]
