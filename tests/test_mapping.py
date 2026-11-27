"""Lookup hits and misses for country, segment, and status."""

from __future__ import annotations

import pytest

from etl_validation.transform import classify_row
from tests.helpers import source_row

pytestmark = pytest.mark.unit


def test_known_codes_map_to_lookup_names(lookups):
    accepted, rejected = classify_row(
        source_row(country_code="gb", segment_code="p", status_code="i"),
        lookups,
    )
    assert rejected is None
    assert accepted["country_code"] == "GB"
    assert accepted["country_name"] == "United Kingdom"
    assert accepted["segment"] == "Partner"
    assert accepted["status"] == "Inactive"


@pytest.mark.parametrize(
    ("field", "value", "reason"),
    [
        ("country_code", "ZZ", "unknown_country"),
        ("segment_code", "Q", "unknown_segment"),
        ("status_code", "Z", "unknown_status"),
    ],
)
def test_unknown_codes_are_rejected(lookups, field, value, reason):
    _accepted, rejected = classify_row(source_row(**{field: value}), lookups)
    assert rejected["reject_reason"] == reason

