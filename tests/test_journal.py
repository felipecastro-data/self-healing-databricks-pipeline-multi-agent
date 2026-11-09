"""The classifier regex fallback and the journal report."""

import pytest

from self_healing.journal import build_rounds, parse_classification, render, summarize_runtimes


@pytest.mark.parametrize(
    "reply, expected",
    [
        ("Classification: type_mismatch\nMatched signature: x", "type_mismatch"),
        ("**Classification:** `bad_join`", "bad_join"),
        ("Classification - oom", "oom"),
        ("classification: SCHEMA_DRIFT", "schema_drift"),
        ("Looking at this.\nCategory: null_violation\nDone.", "null_violation"),
        ("Classification: unclassified\nMatched signature: none", "unclassified"),
    ],
)
def test_parse_classification(reply, expected):
    assert parse_classification(reply) == expected


@pytest.mark.parametrize("reply", ["no label here", "Classification: made_up_category"])
def test_parse_classification_refuses_unknown(reply):
    with pytest.raises(ValueError):
        parse_classification(reply)


def _agent(name, ts, output):
    return {"agent_name": name, "timestamp": f"2026-09-16T{ts}Z", "output": output}


def _tool(name, ts, output="{}"):
    return {"tool_name": f"mcp__server__{name}", "timestamp": f"2026-09-16T{ts}Z", "output": output}


def _round(start, ts_classifier, ts_patch):
    return [
        _agent("log-parser", start, "excerpt"),
        _agent("classifier", ts_classifier, "Classification: schema_drift\nContract clause: columns.a.present"),
        _agent("patch-proposer", ts_patch, "Category: schema_drift\nTarget: src/jobs/contract_drift.py:3\nPatch:"),
    ]


def test_build_rounds_tracks_each_round_to_its_rerun():
    rows = [
        _tool("list_runs", "10:00:00"),
        *_round("10:00:10", "10:00:20", "10:00:35"),
        _tool("apply_patch", "10:01:00"),
        _tool("run_job", "10:02:00", '{"job_name":"j","state":"FAILED"}'),
        *_round("10:03:00", "10:03:10", "10:03:20"),
    ]
    first, second = build_rounds(rows)
    assert first["scenario"] == "contract_drift"
    assert first["clause"] == "columns.a.present"
    assert first["diagnosis_s"] == 25
    assert (first["applied"], first["outcome"], first["verify_s"]) == (True, "FAILED", 110)
    assert (second["applied"], second["outcome"]) == (False, None)


def test_run_before_apply_is_not_a_verification():
    rows = [*_round("10:00:00", "10:00:01", "10:00:02"), _tool("run_job", "10:00:03", '{"state":"SUCCESS"}')]
    assert build_rounds(rows)[0]["outcome"] is None


def test_unparsable_classifier_output_does_not_break_the_report():
    rows = [_agent("log-parser", "10:00:00", "x"), _agent("classifier", "10:00:01", "garbled")]
    assert build_rounds(rows)[0]["category"] == "unparsed"


def _run(job, start, state, seconds):
    return {"job_name": job, "start_time": start, "result_state": state, "run_duration_s": seconds}


def test_runtimes_compare_last_failure_with_first_success():
    history = [_run("j", "1", "FAILED", 40), _run("j", "2", "FAILED", 50), _run("j", "3", "SUCCESS", 60)]
    history.append(_run("j", "4", "SUCCESS", 500))
    assert summarize_runtimes(history)["j"] == {
        "failed_s": 50,
        "fixed_s": 60,
        "latest_state": "SUCCESS",
        "latest_s": 500,
    }


def test_runtimes_never_fixed_and_never_failed():
    summary = summarize_runtimes([_run("broken", "1", "FAILED", 90), _run("good", "1", "SUCCESS", 20)])
    assert (summary["broken"]["failed_s"], summary["broken"]["fixed_s"]) == (90, None)
    assert (summary["good"]["failed_s"], summary["good"]["fixed_s"]) == (None, None)


def test_render_handles_empty_evidence():
    assert "Diagnostic rounds: **0**" in render([], {})
