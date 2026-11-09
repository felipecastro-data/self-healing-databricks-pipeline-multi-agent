"""Load the YAML data contracts in contracts/ and validate DataFrames against them."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from self_healing.jobs.checks import check_expected_columns, check_not_null, check_unique_key

CONTRACTS_DIR = Path(__file__).resolve().parents[3] / "contracts"


def load_contract(name: str) -> dict[str, Any]:
    """Read contracts/<name>.yml."""
    return yaml.safe_load((CONTRACTS_DIR / f"{name}.yml").read_text())


def validate_contract(df: DataFrame, contract: dict[str, Any]) -> None:
    """Raise DataQualityError at the first broken clause: schema, then not-null, then key uniqueness."""
    name = contract["name"]
    columns = contract["columns"]
    actual = {field.name: field.dataType.simpleString() for field in df.schema.fields}
    check_expected_columns(actual, {column: spec["type"] for column, spec in columns.items()}, name)

    not_null = [column for column, spec in columns.items() if not spec.get("nullable", True)]
    key = contract["primary_key"]
    row = df.agg(
        F.count(F.lit(1)).alias("__total"),
        F.countDistinct(*key).alias("__distinct"),
        *[(F.count(F.lit(1)) - F.count(column)).alias(f"__nulls_{column}") for column in not_null],
    ).first()

    check_not_null({column: row[f"__nulls_{column}"] for column in not_null}, name)
    check_unique_key(row["__total"], row["__distinct"], "primary_key.unique", name)
