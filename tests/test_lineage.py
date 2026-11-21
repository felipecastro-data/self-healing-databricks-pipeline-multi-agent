"""get_table_lineage's query and row mapping, and how the trigger hands lineage to the chain."""

from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.sql import StatementState

from self_healing import orchestrate
from self_healing.mcp import databricks_mcp


def _workspace(monkeypatch, state=StatementState.SUCCEEDED, rows=None):
    w = MagicMock()
    resp = w.statement_execution.execute_statement.return_value
    resp.status.state = state
    resp.status.error.message = "TABLE_OR_VIEW_NOT_FOUND"
    resp.result.data_array = rows
    w.jobs.get.return_value.settings.name = "self-healing-ingest-orders-raw"
    monkeypatch.setattr(databricks_mcp, "_client", lambda: w)
    monkeypatch.setattr(databricks_mcp, "_resolve_job_id", lambda w, name: 42)
    monkeypatch.setenv("DATABRICKS_WAREHOUSE_ID", "wh")
    return w


def test_lineage_names_upstream_table_and_job(monkeypatch):
    rows = [
        ["cat.s.orders_raw", "samples.tpch.orders", "7", "2026-11-09 14:21:07"],
        ["cat.s.lookup", None, None, None],
    ]
    w = _workspace(monkeypatch, rows=rows)
    result = databricks_mcp.get_table_lineage("self-healing-contract-drift")

    assert result == [
        {
            "input_table": "cat.s.orders_raw",
            "upstream_table": "samples.tpch.orders",
            "upstream_job": "self-healing-ingest-orders-raw",
            "last_written": "2026-11-09 14:21:07",
        },
        {"input_table": "cat.s.lookup", "upstream_table": None, "upstream_job": None, "last_written": None},
    ]
    kwargs = w.statement_execution.execute_statement.call_args.kwargs
    assert "system.access.table_lineage" in kwargs["statement"] and "42" not in kwargs["statement"]
    assert {(p.name, p.value) for p in kwargs["parameters"]} == {("job_id", "42"), ("days", "90")}


def test_lineage_failed_query_raises(monkeypatch):
    _workspace(monkeypatch, state=StatementState.FAILED)
    with pytest.raises(RuntimeError, match="TABLE_OR_VIEW_NOT_FOUND"):
        databricks_mcp.get_table_lineage("j")


@pytest.mark.parametrize("days", [0, 366])
def test_lineage_rejects_window_out_of_range(days):
    with pytest.raises(ValueError):
        databricks_mcp.get_table_lineage("j", days=days)


def test_trigger_passes_lineage_to_chain(monkeypatch):
    row = {"input_table": "a", "upstream_table": "b", "upstream_job": "ingest", "last_written": "t"}
    monkeypatch.setattr(orchestrate.databricks_mcp, "get_table_lineage", lambda name: [row])
    prompt = orchestrate._build_diagnostic_prompt("j", 1, None, "boom", orchestrate._format_lineage("j"))
    assert "a <- b (job ingest, last written t)" in prompt


def test_trigger_diagnoses_without_lineage(monkeypatch):
    def fail(name):
        raise RuntimeError("DATABRICKS_WAREHOUSE_ID is not set")

    monkeypatch.setattr(orchestrate.databricks_mcp, "get_table_lineage", fail)
    assert orchestrate._format_lineage("j") == "unavailable (RuntimeError: DATABRICKS_WAREHOUSE_ID is not set)"
