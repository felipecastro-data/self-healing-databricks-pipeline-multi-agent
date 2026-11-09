"""Data quality checks shared by the Databricks jobs.

Each check takes plain numbers or mappings, so it can be tested without Spark,
and raises DataQualityError on failure. The error text is a fixed set of
key=value fields that the diagnostic agents key on (see CLAUDE.md section 2).
Unlike `assert`, these still run under `python -O`.
"""

from __future__ import annotations

from collections.abc import Mapping


class DataQualityError(Exception):
    """A dataset broke a declared rule. `clause` names the exact rule."""

    def __init__(self, check: str, clause: str, detail: str, contract: str = "none") -> None:
        self.check = check
        self.clause = clause
        self.detail = detail
        self.contract = contract
        super().__init__(f"check={check} contract={contract} clause={clause} detail={detail}")


def check_expected_columns(actual: Mapping[str, str], expected: Mapping[str, str], contract: str = "none") -> None:
    """Require every expected column with its expected type. Extra columns are allowed."""
    for name, expected_type in expected.items():
        if name not in actual:
            raise DataQualityError(
                "expected_columns", f"columns.{name}.present", f"available={sorted(actual)}", contract
            )
        if actual[name] != expected_type:
            raise DataQualityError(
                "expected_columns",
                f"columns.{name}.type",
                f"expected={expected_type} actual={actual[name]}",
                contract,
            )


def check_not_null(null_counts: Mapping[str, int], contract: str = "none") -> None:
    """Require a null count of zero for every column given."""
    for name, count in null_counts.items():
        if count > 0:
            raise DataQualityError("not_null", f"columns.{name}.not_null", f"null_rows={count}", contract)


def check_unique_key(total_rows: int, distinct_keys: int, clause: str, contract: str = "none") -> None:
    """Require one row per key value."""
    if total_rows != distinct_keys:
        raise DataQualityError("unique_key", clause, f"rows={total_rows} distinct_keys={distinct_keys}", contract)


def check_row_count_ratio(input_rows: int, output_rows: int, max_ratio: float) -> None:
    """Fail when a transformation multiplies the row count beyond max_ratio."""
    if output_rows > input_rows * max_ratio:
        raise DataQualityError(
            "row_count_ratio", "row_count.max_ratio", f"input={input_rows} output={output_rows} max_ratio={max_ratio}"
        )
