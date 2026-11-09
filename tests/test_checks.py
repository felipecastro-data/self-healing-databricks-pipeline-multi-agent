"""Data quality checks: each passes on good input and names the broken clause on bad input."""

import pytest

from self_healing.jobs.checks import (
    DataQualityError,
    check_expected_columns,
    check_not_null,
    check_row_count_ratio,
    check_unique_key,
)


def test_expected_columns_allows_extras():
    check_expected_columns({"a": "int", "extra": "string"}, {"a": "int"})


def test_missing_column_names_the_clause():
    with pytest.raises(DataQualityError) as err:
        check_expected_columns({"b": "int"}, {"a": "int"}, "orders_raw")
    assert err.value.clause == "columns.a.present"
    assert "check=expected_columns contract=orders_raw clause=columns.a.present" in str(err.value)


def test_wrong_type_names_the_clause():
    with pytest.raises(DataQualityError) as err:
        check_expected_columns({"a": "string"}, {"a": "int"})
    assert err.value.clause == "columns.a.type"


def test_not_null():
    check_not_null({"a": 0})
    with pytest.raises(DataQualityError) as err:
        check_not_null({"a": 0, "b": 3})
    assert err.value.clause == "columns.b.not_null"
    assert err.value.detail == "null_rows=3"


def test_unique_key():
    check_unique_key(5, 5, "primary_key.unique")
    with pytest.raises(DataQualityError):
        check_unique_key(5, 4, "primary_key.unique")


@pytest.mark.parametrize("output_rows, fails", [(100, False), (101, True)])
def test_row_count_ratio_boundary(output_rows, fails):
    if fails:
        with pytest.raises(DataQualityError):
            check_row_count_ratio(10, output_rows, 10)
    else:
        check_row_count_ratio(10, output_rows, 10)
