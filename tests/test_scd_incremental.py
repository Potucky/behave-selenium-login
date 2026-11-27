"""SCD2 outcomes of the two sample batches."""

from __future__ import annotations

from decimal import Decimal

import duckdb
import pytest

from etl_validation.load import fetch_dimension, fetch_quarantine

pytestmark = pytest.mark.integration


def _versions(loaded, customer_id: str) -> list[dict]:
    connection = duckdb.connect(str(loaded.target_db), read_only=True)
    try:
        rows = fetch_dimension(connection)
    finally:
        connection.close()
    return [row for row in rows if row["customer_id"] == customer_id]


def _current(loaded, customer_id: str) -> dict:
    versions = _versions(loaded, customer_id)
    current = [row for row in versions if row["is_current"]]
    assert len(current) == 1
    return current[0]


def test_sample_batch_files_load_in_lexical_order(config):
    assert [path.name for path in config.source_files] == [
        "customers_20260115.csv",
        "customers_20260122.csv",
    ]


def test_history_has_eight_current_and_eleven_total_rows(loaded):
    connection = duckdb.connect(str(loaded.target_db), read_only=True)
    try:
        rows = fetch_dimension(connection)
    finally:
        connection.close()
    assert len(rows) == 11
    assert sum(row["is_current"] for row in rows) == 8


def test_unchanged_customer_keeps_the_original_version(loaded):
    versions = _versions(loaded, "C001")
    assert len(versions) == 1
    current = versions[0]
    assert current["email"] == "alice@example.com"
    assert current["load_batch_id"] == "customers_20260115"
    assert current["is_current"] is True
    assert str(current["effective_to"]) == "9999-12-31"


def test_attribute_change_closes_the_previous_version(loaded):
    versions = _versions(loaded, "C002")
    assert len(versions) == 2
    closed, current = versions
    assert closed["email"] == "bob@example.com"
    assert closed["is_current"] is False
    assert closed["effective_to"].isoformat() == "2026-01-22"
    assert current["email"] == "bob.singh@example.com"
    assert current["is_current"] is True
    assert current["effective_from"].isoformat() == "2026-01-22"
    assert current["load_batch_id"] == "customers_20260122"
    assert closed["effective_to"] == current["effective_from"]


def test_credit_limit_change_keeps_the_closed_amount(loaded):
    versions = _versions(loaded, "C003")
    assert versions[0]["credit_limit"] == Decimal("2500.50")
    assert versions[1]["credit_limit"] == Decimal("2600.50")
    assert versions[1]["is_current"] is True


def test_status_change_opens_a_new_version(loaded):
    versions = _versions(loaded, "C004")
    assert versions[0]["status"] == "Active"
    assert versions[1]["status"] == "Inactive"
    assert versions[1]["is_current"] is True


def test_in_batch_duplicate_survives_as_one_current_row(loaded):
    versions = _versions(loaded, "C005")
    assert len(versions) == 1
    assert versions[0]["email"] == "eva@example.com"
    assert versions[0]["credit_limit"] == Decimal("300.00")


def test_late_arriving_row_does_not_overwrite_the_current_version(loaded):
    current = _current(loaded, "C010")
    assert current["email"] == "frank@example.com"
    assert current["full_name"] == "Frank Müller"
    assert current["load_batch_id"] == "customers_20260115"
    assert current["source_updated_at"].isoformat(sep=" ") == "2026-01-15 09:00:00"


def test_cleaned_text_and_amount_land_on_the_current_row(loaded):
    current = _current(loaded, "C011")
    assert current["full_name"] == "Grace Hopper"
    assert current["email"] == "grace@example.com"
    assert current["phone"] == "4155550107"
    assert current["credit_limit"] == Decimal("1500.00")
    assert current["segment"] == "Retail"


def test_new_customer_in_the_second_batch_is_inserted(loaded):
    current = _current(loaded, "C013")
    assert current["full_name"] == "Helen Cho"
    assert current["country_name"] == "South Korea"
    assert current["load_batch_id"] == "customers_20260122"
    assert len(_versions(loaded, "C013")) == 1


def test_loaded_dimension_stores_lookup_names_not_source_codes(loaded):
    connection = duckdb.connect(str(loaded.target_db), read_only=True)
    try:
        rows = fetch_dimension(connection)
    finally:
        connection.close()
    assert {row["segment"] for row in rows} <= {"Retail", "Wholesale", "Partner"}
    assert {row["status"] for row in rows} <= {"Active", "Inactive"}
    assert "R" not in {row["segment"] for row in rows}
    assert "A" not in {row["status"] for row in rows}
    assert _current(loaded, "C003")["country_name"] == "United Kingdom"


def test_rejected_customers_are_absent_from_the_dimension(loaded):
    connection = duckdb.connect(str(loaded.target_db), read_only=True)
    try:
        present = {row["customer_id"] for row in fetch_dimension(connection)}
        reasons = {row["reject_reason"] for row in fetch_quarantine(connection)}
    finally:
        connection.close()
    assert {"C006", "C007", "C008", "C012", "C014"}.isdisjoint(present)
    assert reasons == {
        "invalid_email",
        "missing_customer_id",
        "invalid_credit_limit",
        "unknown_country",
        "unknown_segment",
        "invalid_phone",
    }
