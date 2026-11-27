"""Source-to-target counts, checksums, and the checks that fire on a bad warehouse."""

from __future__ import annotations

import duckdb
import pytest

from etl_validation.load import fetch_control, fetch_quarantine
from etl_validation.reconcile import build_report

pytestmark = pytest.mark.integration


def _check(report, check_id: str):
    return next(check for check in report.checks if check.check_id == check_id)


def test_reconciliation_passes_for_the_sample_warehouse(loaded):
    report = build_report(loaded)
    assert report.passed
    assert [check.check_id for check in report.checks] == [
        "current_row_count",
        "history_row_count",
        "current_checksum",
        "current_attributes",
        "scd_history",
        "scd_one_current",
        "scd_no_overlap",
        "scd_contiguous",
        "domain_values",
        "data_types",
        "record_hash_integrity",
        "quarantine",
        "control_totals",
    ]
    assert _check(report, "current_row_count").expected == "8"
    assert _check(report, "history_row_count").expected == "11"
    assert len(_check(report, "current_checksum").expected) == 64


def test_control_totals_match_the_documented_batch_counts(loaded):
    connection = duckdb.connect(str(loaded.target_db), read_only=True)
    try:
        stats = {item.batch_id: item for item in fetch_control(connection)}
        quarantine = fetch_quarantine(connection)
    finally:
        connection.close()

    first = stats["customers_20260115"]
    assert (first.extracted_rows, first.duplicate_rows, first.accepted_rows, first.rejected_rows) == (13, 1, 7, 5)
    assert (first.inserted_rows, first.versioned_rows, first.unchanged_rows, first.skipped_late_rows) == (7, 0, 0, 0)

    second = stats["customers_20260122"]
    assert (second.extracted_rows, second.duplicate_rows, second.accepted_rows, second.rejected_rows) == (8, 1, 6, 1)
    assert (second.inserted_rows, second.versioned_rows, second.unchanged_rows, second.skipped_late_rows) == (1, 3, 1, 1)
    assert len(quarantine) == 6


def test_attribute_and_checksum_checks_list_a_tampered_email(loaded):
    connection = duckdb.connect(str(loaded.target_db))
    connection.execute(
        """
        UPDATE dim_customer
        SET email = 'tampered@example.com'
        WHERE customer_id = 'C003' AND is_current
        """
    )
    connection.execute("CHECKPOINT")
    connection.close()

    report = build_report(loaded)
    assert not report.passed
    attributes = _check(report, "current_attributes")
    assert not attributes.passed
    assert any(
        item.key == "C003" and item.column == "email" and item.actual == "tampered@example.com"
        for item in attributes.mismatches
    )
    assert not _check(report, "current_checksum").passed
    assert not _check(report, "record_hash_integrity").passed
    integrity = _check(report, "record_hash_integrity")
    assert any(item.key.startswith("C003@") and item.column == "record_hash" for item in integrity.mismatches)


def test_data_type_check_catches_a_non_numeric_phone(loaded):
    connection = duckdb.connect(str(loaded.target_db))
    connection.execute(
        "UPDATE dim_customer SET phone = 'abc' WHERE customer_id = 'C001' AND is_current"
    )
    connection.execute("CHECKPOINT")
    connection.close()

    report = build_report(loaded)
    mismatches = _check(report, "data_types").mismatches
    assert any(item.key == "C001" and item.column == "phone" and item.actual == "abc" for item in mismatches)


def test_domain_check_catches_a_segment_outside_the_lookup(loaded):
    connection = duckdb.connect(str(loaded.target_db))
    connection.execute(
        "UPDATE dim_customer SET segment = 'Nope' WHERE customer_id = 'C001' AND is_current"
    )
    connection.execute("CHECKPOINT")
    connection.close()

    report = build_report(loaded)
    mismatches = _check(report, "domain_values").mismatches
    assert any(item.key == "C001" and item.column == "segment" and item.actual == "Nope" for item in mismatches)


def test_overlap_check_catches_a_closed_row_left_open(loaded):
    connection = duckdb.connect(str(loaded.target_db))
    connection.execute(
        """
        UPDATE dim_customer
        SET effective_to = DATE '9999-12-31'
        WHERE customer_id = 'C002' AND NOT is_current
        """
    )
    connection.execute("CHECKPOINT")
    connection.close()

    report = build_report(loaded)
    assert any(item.key == "C002" for item in _check(report, "scd_no_overlap").mismatches)
    assert any(item.key == "C002" for item in _check(report, "scd_contiguous").mismatches)
