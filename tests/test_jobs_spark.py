"""Contracts and jobs against a real local Spark session. Needs Java."""

import datetime
import sys
from decimal import Decimal

import pytest
from pyspark.sql import SparkSession

from self_healing.jobs import golden_job, ingest_orders_raw
from self_healing.jobs.broken_scenarios import bad_join, contract_drift, null_violation
from self_healing.jobs.checks import DataQualityError
from self_healing.jobs.contract import load_contract, validate_contract

SCHEMA = "ddl_test"
DDL = (
    "o_orderkey BIGINT, o_custkey BIGINT, o_orderstatus STRING, o_totalprice DECIMAL(18,2), o_orderdate DATE, "
    "o_orderpriority STRING, o_clerk STRING, o_shippriority INT, o_comment STRING"
)


def _row(key, cust=1, price="10.00", status="O"):
    return (key, cust, status, Decimal(price) if price else None, datetime.date(1995, 1, 1), "1", "c", 0, "x")


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    session = (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.warehouse.dir", str(tmp_path_factory.mktemp("warehouse")))
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sql(f"CREATE DATABASE IF NOT EXISTS {SCHEMA}")
    yield session
    session.stop()


@pytest.fixture
def orders(spark):
    frame = spark.createDataFrame([_row(1), _row(2), _row(3, cust=2)], DDL)
    frame.write.mode("overwrite").saveAsTable(f"{SCHEMA}.orders_raw")
    return frame


def test_valid_data_passes(orders):
    validate_contract(orders, load_contract("orders_raw"))


def test_null_breach_cites_clause(spark):
    frame = spark.createDataFrame([_row(1, price=None)], DDL)
    with pytest.raises(DataQualityError) as err:
        validate_contract(frame, load_contract("orders_raw"))
    assert err.value.clause == "columns.o_totalprice.not_null"


def test_duplicate_key_breach_cites_clause(spark):
    frame = spark.createDataFrame([_row(1), _row(1)], DDL)
    with pytest.raises(DataQualityError) as err:
        validate_contract(frame, load_contract("orders_raw"))
    assert err.value.clause == "primary_key.unique"


def test_golden_job_writes_summary(spark, orders):
    golden_job.run(spark, SCHEMA)
    rows = {r.o_custkey: r.order_count for r in spark.table(f"{SCHEMA}.orders_summary").collect()}
    assert rows == {1: 2, 2: 1}


def test_ingest_validates_and_writes(spark, orders):
    spark.createDataFrame([_row(9)], DDL).write.mode("overwrite").saveAsTable(f"{SCHEMA}.source_orders")
    ingest_orders_raw.run(spark, SCHEMA, source_table=f"{SCHEMA}.source_orders")
    assert [r.o_orderkey for r in spark.table(f"{SCHEMA}.orders_raw").collect()] == [9]


def test_contract_drift_fix_restores_the_contracted_column(spark, orders):
    contract_drift.run(spark, SCHEMA)
    assert spark.table(f"{SCHEMA}.orders_summary_contract_drift").count() == 2


def test_null_violation_fix_drops_null_totals(spark, monkeypatch):
    frame = spark.createDataFrame([_row(key) for key in range(1, 5)] + [_row(5, cust=2)], DDL)
    frame.write.mode("overwrite").saveAsTable(f"{SCHEMA}.orders_raw")
    # The real target table is Delta, which local Spark lacks.
    target = f"{SCHEMA}.orders_summary_null_violation"
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {target} "
        "(o_custkey BIGINT, total_value DECIMAL(18,2), order_count BIGINT) USING PARQUET"
    )
    monkeypatch.setattr(null_violation, "ensure_target_table", lambda *args: None)
    null_violation.run(spark, SCHEMA)
    assert [row.o_custkey for row in spark.table(target).collect()] == [1]


def test_bad_join_dedup_prevents_explosion(spark, orders):
    bad_join.run(spark, SCHEMA)
    assert spark.table(f"{SCHEMA}.orders_summary_bad_join").count() == 2


def test_entry_point_runs_the_named_job(spark, orders, monkeypatch):
    import databricks_entry

    monkeypatch.setattr(sys, "argv", ["entry", "--src", "src", "--job", "golden_job", "--schema", SCHEMA])
    databricks_entry.main(spark)
    assert spark.table(f"{SCHEMA}.orders_summary").count() == 2
