"""Which duplicate source row survives inside a batch."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from etl_validation.transform import deduplicate, prepare_batch
from tests.helpers import source_frame, source_row

pytestmark = pytest.mark.unit


def _accepted_pair(lookups, first: dict, second: dict):
    accepted, rejected = prepare_batch(source_frame([first, second]), lookups)
    assert rejected.empty
    assert len(accepted) == 1
    return accepted.iloc[0]


def test_duplicate_customer_keeps_latest_source_updated_at(lookups):
    winner = _accepted_pair(
        lookups,
        source_row(
            customer_id="C005",
            email="eva.old@example.com",
            source_updated_at="2026-01-14T08:00:00Z",
            source_row_number=1,
        ),
        source_row(
            customer_id="C005",
            email="eva@example.com",
            source_updated_at="2026-01-15T11:00:00Z",
            source_row_number=2,
        ),
    )
    assert winner["email"] == "eva@example.com"
    assert winner["source_row_number"] == 2


def test_duplicate_tie_keeps_the_later_source_row(lookups):
    same_time = "2026-01-15T11:00:00Z"
    winner = _accepted_pair(
        lookups,
        source_row(customer_id="C005", email="first@example.com", source_updated_at=same_time, source_row_number=4),
        source_row(customer_id="C005", email="second@example.com", source_updated_at=same_time, source_row_number=9),
    )
    assert winner["email"] == "second@example.com"


def test_deduplicate_orders_by_timestamp_before_row_number():
    shared = {
        "full_name": "Eva Rossi",
        "phone": "39065550105",
        "country_code": "IT",
        "country_name": "Italy",
        "segment": "Retail",
        "status": "Active",
        "credit_limit": "100.00",
        "effective_from": datetime(2026, 1, 15, tzinfo=timezone.utc).date(),
        "source_file": "unit.csv",
        "record_hash": "hash",
    }
    frame = source_frame(
        [
            {
                **shared,
                "customer_id": "C005",
                "email": "later-row-but-older@example.com",
                "source_updated_at": datetime(2026, 1, 14, 8, 0, tzinfo=timezone.utc),
                "source_row_number": 20,
            },
            {
                **shared,
                "customer_id": "C005",
                "email": "earlier-row-but-newer@example.com",
                "source_updated_at": datetime(2026, 1, 15, 11, 0, tzinfo=timezone.utc),
                "source_row_number": 1,
            },
        ]
    )
    winner = deduplicate(frame).iloc[0]
    assert winner["email"] == "earlier-row-but-newer@example.com"
