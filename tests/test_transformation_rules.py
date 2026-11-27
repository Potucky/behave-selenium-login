"""Cleaning rules for names, emails, phones, amounts, and reject priority."""

from __future__ import annotations

import pandas as pd
import pytest

from etl_validation.transform import classify_row, prepare_batch
from tests.helpers import source_frame, source_row

pytestmark = pytest.mark.unit


def test_email_is_trimmed_and_lowercased(lookups):
    accepted, rejected = classify_row(source_row(email="  GRACE@EXAMPLE.COM "), lookups)
    assert rejected is None
    assert accepted["email"] == "grace@example.com"


def test_name_collapses_whitespace_and_uses_title_case(lookups):
    accepted, rejected = classify_row(source_row(full_name="  grace   hopper "), lookups)
    assert rejected is None
    assert accepted["full_name"] == "Grace Hopper"


def test_customer_id_is_trimmed_and_uppercased(lookups):
    accepted, rejected = classify_row(source_row(customer_id=" c100 "), lookups)
    assert rejected is None
    assert accepted["customer_id"] == "C100"


def test_credit_limit_strips_currency_and_thousands_separators(lookups):
    accepted, rejected = classify_row(source_row(credit_limit="$1,500.00"), lookups)
    assert rejected is None
    assert str(accepted["credit_limit"]) == "1500.00"


def test_credit_limit_with_more_than_two_decimal_places_is_rejected(lookups):
    _accepted, rejected = classify_row(source_row(credit_limit="10.555"), lookups)
    assert rejected["reject_reason"] == "invalid_credit_limit"


def test_negative_credit_limit_is_rejected(lookups):
    _accepted, rejected = classify_row(source_row(credit_limit="-5.00"), lookups)
    assert rejected["reject_reason"] == "invalid_credit_limit"


def test_blank_name_is_rejected(lookups):
    _accepted, rejected = classify_row(source_row(full_name="   "), lookups)
    assert rejected["reject_reason"] == "missing_full_name"


def test_customer_id_with_internal_space_is_rejected(lookups):
    _accepted, rejected = classify_row(source_row(customer_id="C 100"), lookups)
    assert rejected["reject_reason"] == "invalid_customer_id"


def test_phone_keeps_digits_only(lookups):
    accepted, rejected = classify_row(source_row(phone="(415) 555-0107"), lookups)
    assert rejected is None
    assert accepted["phone"] == "4155550107"


def test_phone_outside_10_to_15_digits_is_rejected(lookups):
    _accepted, rejected = classify_row(source_row(phone="123"), lookups)
    assert rejected["reject_reason"] == "invalid_phone"


def test_invalid_timestamp_is_rejected(lookups):
    _accepted, rejected = classify_row(source_row(source_updated_at="not-a-date"), lookups)
    assert rejected["reject_reason"] == "invalid_source_updated_at"


def test_first_reject_reason_wins_when_several_rules_fail(lookups):
    _accepted, rejected = classify_row(
        source_row(email="not-an-email", country_code="ZZ", credit_limit="-1"),
        lookups,
    )
    assert rejected["reject_reason"] == "invalid_email"


def test_missing_customer_id_wins_over_a_bad_email(lookups):
    _accepted, rejected = classify_row(source_row(customer_id="  ", email="not-an-email"), lookups)
    assert rejected["reject_reason"] == "missing_customer_id"
    assert rejected["customer_id"] == ""


def test_invalid_duplicate_does_not_block_a_valid_row(lookups):
    frame = source_frame(
        [
            source_row(customer_id="C100", email="not-an-email", source_row_number=1),
            source_row(customer_id="C100", email="ok@example.com", source_row_number=2),
        ]
    )
    accepted, rejected = prepare_batch(frame, lookups)
    assert rejected["reject_reason"].tolist() == ["invalid_email"]
    assert accepted["customer_id"].tolist() == ["C100"]
    assert accepted["email"].tolist() == ["ok@example.com"]


def test_quarantine_payload_keeps_the_raw_source_value(lookups):
    _accepted, rejected = classify_row(source_row(email="not-an-email"), lookups)
    assert "not-an-email" in rejected["raw_payload"]
    assert "source_updated_at" in rejected["raw_payload"]


def test_accepted_frame_uses_decimal_credit_and_a_date(lookups):
    accepted, _rejected = prepare_batch(source_frame([source_row(credit_limit="2,500.50")]), lookups)
    assert str(accepted.loc[0, "credit_limit"]) == "2500.50"
    assert accepted.loc[0, "effective_from"].isoformat() == "2026-01-15"
    assert len(accepted.loc[0, "record_hash"]) == 64
