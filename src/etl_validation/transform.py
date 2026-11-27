"""Cleaning rules, reject reasons, deduplication, and the attribute hash.

The warehouse load and the reconciliation replay both call this module, so the
field rules live in one place. SCD history is implemented separately: SQL in
the load, and a row-by-row fold that reconciliation treats as the expected
result.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal, InvalidOperation

import pandas as pd

CUSTOMER_ID_RE = re.compile(r"[A-Z0-9]+")
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"\d{10,15}")

RAW_COLUMNS = (
    "customer_id",
    "full_name",
    "email",
    "phone",
    "country_code",
    "segment_code",
    "status_code",
    "credit_limit",
    "source_updated_at",
)

ACCEPTED_COLUMNS = (
    "customer_id",
    "full_name",
    "email",
    "phone",
    "country_code",
    "country_name",
    "segment",
    "status",
    "credit_limit",
    "source_updated_at",
    "effective_from",
    "source_file",
    "source_row_number",
    "record_hash",
)

REJECT_COLUMNS = (
    "source_file",
    "source_row_number",
    "customer_id",
    "reject_reason",
    "raw_payload",
)

# First matching reason wins, so a row with several defects has one stable reason.
REJECT_PRIORITY = (
    "missing_customer_id",
    "invalid_customer_id",
    "missing_full_name",
    "invalid_email",
    "invalid_phone",
    "invalid_credit_limit",
    "unknown_country",
    "unknown_segment",
    "unknown_status",
    "invalid_source_updated_at",
)

HASH_FIELDS = (
    "full_name",
    "email",
    "phone",
    "country_code",
    "country_name",
    "segment",
    "status",
    "credit_limit",
)


@dataclass(frozen=True)
class Lookups:
    countries: dict[str, str]
    segments: dict[str, str]
    statuses: dict[str, str]


def normalize_customer_id(value: object) -> str:
    return str(value).strip().upper()


def normalize_name(value: object) -> str:
    """Collapse whitespace and apply simple title case.

    This is a demo rule. It is the wrong choice for names such as McDonald;
    the sample data does not contain those cases.
    """
    return " ".join(str(value).split()).title()


def normalize_email(value: object) -> str:
    return str(value).strip().lower()


def normalize_phone(value: object) -> str:
    return "".join(character for character in str(value) if character.isdigit())


def parse_credit_limit(value: object) -> Decimal | None:
    """Parse a currency amount. More than two decimal places is a reject, not a round."""
    text = str(value).strip().replace("$", "").replace(",", "")
    if not text:
        return None
    try:
        amount = Decimal(text)
    except InvalidOperation:
        return None
    if amount < 0:
        return None
    quantized = amount.quantize(Decimal("0.01"))
    if amount != quantized:
        return None
    return quantized


def parse_source_updated_at(value: object) -> datetime | None:
    text = str(value).strip()
    if not text:
        return None
    parsed = pd.to_datetime(text, utc=True, errors="coerce")
    if pd.isna(parsed):
        return None
    return parsed.to_pydatetime()


def format_credit(value: object) -> str:
    return f"{Decimal(str(value)).quantize(Decimal('0.01')):.2f}"


def record_hash(row: dict) -> str:
    """SHA-256 of the tracked attributes. The business key is not part of the hash."""
    parts = []
    for field in HASH_FIELDS:
        if field == "credit_limit":
            parts.append(format_credit(row[field]))
        else:
            parts.append(str(row[field]))
    payload = "\x1f".join(parts)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def aggregate_checksum(pairs: list[tuple[str, str]]) -> str:
    """Checksum of customer_id plus attribute hash, ordered by customer_id."""
    lines = [f"{customer_id}|{digest}" for customer_id, digest in sorted(pairs)]
    return hashlib.sha256("\n".join(lines).encode("utf-8")).hexdigest()


def accepted_checksum(frame: pd.DataFrame) -> str:
    if frame.empty:
        pairs: list[tuple[str, str]] = []
    else:
        pairs = [(str(row.customer_id), str(row.record_hash)) for row in frame.itertuples(index=False)]
    return aggregate_checksum(pairs)


def _reject_reason(row: dict, lookups: Lookups) -> str | None:
    customer_id = normalize_customer_id(row["customer_id"])
    if not customer_id:
        return "missing_customer_id"
    if CUSTOMER_ID_RE.fullmatch(customer_id) is None:
        return "invalid_customer_id"
    if not normalize_name(row["full_name"]):
        return "missing_full_name"
    if EMAIL_RE.fullmatch(normalize_email(row["email"])) is None:
        return "invalid_email"
    if PHONE_RE.fullmatch(normalize_phone(row["phone"])) is None:
        return "invalid_phone"
    if parse_credit_limit(row["credit_limit"]) is None:
        return "invalid_credit_limit"
    if str(row["country_code"]).strip().upper() not in lookups.countries:
        return "unknown_country"
    if str(row["segment_code"]).strip().upper() not in lookups.segments:
        return "unknown_segment"
    if str(row["status_code"]).strip().upper() not in lookups.statuses:
        return "unknown_status"
    if parse_source_updated_at(row["source_updated_at"]) is None:
        return "invalid_source_updated_at"
    return None


def classify_row(row: dict, lookups: Lookups) -> tuple[dict | None, dict | None]:
    """Return either a cleaned record or a quarantine record."""
    reason = _reject_reason(row, lookups)
    customer_id = normalize_customer_id(row["customer_id"])
    if reason is not None:
        payload = {column: row[column] for column in RAW_COLUMNS}
        return None, {
            "source_file": row["source_file"],
            "source_row_number": int(row["source_row_number"]),
            "customer_id": customer_id,
            "reject_reason": reason,
            "raw_payload": json.dumps(payload, ensure_ascii=False, sort_keys=True),
        }

    source_updated_at = parse_source_updated_at(row["source_updated_at"])
    assert source_updated_at is not None
    country_code = str(row["country_code"]).strip().upper()
    segment_code = str(row["segment_code"]).strip().upper()
    status_code = str(row["status_code"]).strip().upper()
    accepted = {
        "customer_id": customer_id,
        "full_name": normalize_name(row["full_name"]),
        "email": normalize_email(row["email"]),
        "phone": normalize_phone(row["phone"]),
        "country_code": country_code,
        "country_name": lookups.countries[country_code],
        "segment": lookups.segments[segment_code],
        "status": lookups.statuses[status_code],
        "credit_limit": parse_credit_limit(row["credit_limit"]),
        "source_updated_at": source_updated_at,
        "effective_from": source_updated_at.date(),
        "source_file": row["source_file"],
        "source_row_number": int(row["source_row_number"]),
    }
    accepted["record_hash"] = record_hash(accepted)
    return accepted, None


def deduplicate(frame: pd.DataFrame) -> pd.DataFrame:
    """Keep one row per customer_id: latest source_updated_at, then later file row.

    The input is already limited to accepted rows. Rejected duplicates stay in
    quarantine and do not compete here.
    """
    if frame.empty:
        return pd.DataFrame(columns=list(ACCEPTED_COLUMNS))
    ordered = frame.sort_values(
        ["customer_id", "source_updated_at", "source_row_number"],
        ascending=[True, False, False],
        kind="mergesort",
    )
    return ordered.drop_duplicates("customer_id", keep="first").reset_index(drop=True)


def prepare_batch(raw: pd.DataFrame, lookups: Lookups) -> tuple[pd.DataFrame, pd.DataFrame]:
    accepted_rows: list[dict] = []
    rejected_rows: list[dict] = []
    for record in raw.to_dict(orient="records"):
        accepted, rejected = classify_row(record, lookups)
        if rejected is not None:
            rejected_rows.append(rejected)
        else:
            assert accepted is not None
            accepted_rows.append(accepted)
    accepted = deduplicate(pd.DataFrame(accepted_rows, columns=list(ACCEPTED_COLUMNS)))
    rejected = pd.DataFrame(rejected_rows, columns=list(REJECT_COLUMNS))
    return accepted, rejected


def input_counts(extracted_rows: int, accepted: pd.DataFrame, rejected: pd.DataFrame) -> dict[str, int | str]:
    accepted_rows = 0 if accepted.empty else len(accepted)
    rejected_rows = 0 if rejected.empty else len(rejected)
    return {
        "extracted_rows": extracted_rows,
        "duplicate_rows": extracted_rows - rejected_rows - accepted_rows,
        "accepted_rows": accepted_rows,
        "rejected_rows": rejected_rows,
        "accepted_checksum": accepted_checksum(accepted),
    }


def warehouse_timestamp(value: datetime | str) -> str:
    """UTC wall time, without a timezone suffix, for DuckDB TIMESTAMP values."""
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is not None:
        timestamp = timestamp.tz_convert("UTC").tz_localize(None)
    return timestamp.strftime("%Y-%m-%d %H:%M:%S")
