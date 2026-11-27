"""Format checks that do not need a second copy of the cleaning rules."""

from __future__ import annotations

from decimal import Decimal

import pytest

from etl_validation.transform import parse_credit_limit, record_hash

pytestmark = pytest.mark.unit


def test_record_hash_changes_when_a_tracked_attribute_changes():
    base = {
        "full_name": "Alice Nguyen",
        "email": "alice@example.com",
        "phone": "4155550101",
        "country_code": "US",
        "country_name": "United States",
        "segment": "Retail",
        "status": "Active",
        "credit_limit": Decimal("1000.00"),
    }
    changed = {**base, "email": "alice.nguyen@example.com"}
    assert record_hash(base) != record_hash(changed)
    assert record_hash(base) == record_hash({**base, "credit_limit": Decimal("1000")})


def test_parse_credit_limit_accepts_whole_numbers_and_rejects_blanks():
    assert parse_credit_limit("1000") == Decimal("1000.00")
    assert parse_credit_limit("  ") is None
    assert parse_credit_limit("abc") is None
