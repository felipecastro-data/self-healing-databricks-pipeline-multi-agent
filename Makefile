# Loads DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_GIT_FOLDER from .env.
-include .env
export

PY := venv/bin/python
TARGET ?= dev
JOB ?= self-healing-type-mismatch
export BUNDLE_VAR_git_folder ?= $(DATABRICKS_GIT_FOLDER)

.PHONY: setup install deploy ingest demo report test lint

setup: install deploy ingest

install:
	test -x $(PY) || python3 -m venv venv
	$(PY) -m pip install -q -e ".[dev]"

deploy:
	databricks bundle validate -t $(TARGET)
	databricks bundle deploy -t $(TARGET)

ingest:
	databricks bundle run -t $(TARGET) ingest_orders_raw

demo:
	$(PY) -m self_healing.orchestrate $(JOB)

report:
	$(PY) scripts/snapshot_run_history.py
	$(PY) -m self_healing.journal > docs/results.md

test:
	$(PY) -m pytest

lint:
	$(PY) -m ruff check .
