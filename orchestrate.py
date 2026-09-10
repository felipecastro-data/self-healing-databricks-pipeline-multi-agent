"""Deterministic trigger: polls the Databricks Jobs API via the Databricks SDK for failed runs and kicks off the agent diagnostic chain. Polling loop not yet implemented."""

from __future__ import annotations

import re

_KNOWN_CATEGORIES = (
    "schema_drift",
    "null_violation",
    "oom",
    "bad_join",
    "type_mismatch",
    "unclassified",
)

_CLASSIFICATION_LABEL = re.compile(r"(?:Classification|Category)\s*[:\-—]\s*(\w+)", re.IGNORECASE)


def parse_classification(raw_output: str) -> str:
    """Pull the taxonomy category out of a classifier agent response.

    The classifier agent is instructed to reply with only a
    "Classification: <category>" line, but in practice it sometimes wraps
    that in markdown headers, bold text, or a full explanation instead of
    emitting the bare label (observed across repeated test runs even after
    tightening the agent's prompt). Rather than relying on the model to
    self-constrain its output, this strips markdown emphasis/code markers
    and takes the first "Classification:" or "Category:" label found,
    which the classifier consistently places before any surrounding
    narration. The full raw_output should still be written to
    evidence/run_journal.jsonl as-is for audit purposes — this function
    only extracts the field needed to route to patch-proposer.

    Raises ValueError if no label mapping to a known taxonomy category is found.
    """
    stripped = raw_output.replace("*", "").replace("`", "")
    match = _CLASSIFICATION_LABEL.search(stripped)
    if match and match.group(1).lower() in _KNOWN_CATEGORIES:
        return match.group(1).lower()
    raise ValueError("classifier output has no Classification/Category label mapping to a known taxonomy category")
