"""Small source-row builders shared by the unit tests."""

from __future__ import annotations

import pandas as pd


def source_row(**overrides) -> dict:
    row = {
        "customer_id": "C100",
        "full_name": "Test User",
        "email": "test@example.com",
        "phone": "4155550100",
        "country_code": "US",
        "segment_code": "R",
        "status_code": "A",
        "credit_limit": "10.00",
        "source_updated_at": "2026-01-15T10:00:00Z",
        "source_file": "unit.csv",
        "source_row_number": 1,
    }
    row.update(overrides)
    return row


def source_frame(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)
