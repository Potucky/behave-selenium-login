"""Independent SCD2 expectation built by replaying accepted batches in memory.

Reconciliation diffs this history against the DuckDB load. The two paths share
the cleaning rules and do not share the versioning code.
"""

from __future__ import annotations

from datetime import date

import pandas as pd

from etl_validation.extract import PreparedBatch
from etl_validation.models import BatchStats
from etl_validation.transform import input_counts

VERSION_FIELDS = (
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
    "effective_to",
    "is_current",
    "record_hash",
    "load_batch_id",
)


def _open_version(row: dict, batch_id: str, open_ended: date) -> dict:
    return {
        "customer_id": row["customer_id"],
        "full_name": row["full_name"],
        "email": row["email"],
        "phone": row["phone"],
        "country_code": row["country_code"],
        "country_name": row["country_name"],
        "segment": row["segment"],
        "status": row["status"],
        "credit_limit": row["credit_limit"],
        "source_updated_at": row["source_updated_at"],
        "effective_from": row["effective_from"],
        "effective_to": open_ended,
        "is_current": True,
        "record_hash": row["record_hash"],
        "load_batch_id": batch_id,
    }


def _iter_records(frame: pd.DataFrame) -> list[dict]:
    if frame.empty:
        return []
    ordered = frame.sort_values(["customer_id", "source_row_number"], kind="mergesort")
    return ordered.to_dict(orient="records")


def fold(batches: list[PreparedBatch], open_ended: date) -> tuple[list[dict], list[BatchStats]]:
    """Apply each batch in order. Ranges are half-open: [effective_from, effective_to)."""
    history: dict[str, list[dict]] = {}
    stats: list[BatchStats] = []
    for batch in batches:
        inserted = versioned = unchanged = skipped_late = 0
        for row in _iter_records(batch.accepted):
            versions = history.get(row["customer_id"])
            current = versions[-1] if versions else None
            if current is None:
                history.setdefault(row["customer_id"], []).append(
                    _open_version(row, batch.batch_id, open_ended)
                )
                inserted += 1
                continue
            if row["source_updated_at"] <= current["source_updated_at"]:
                skipped_late += 1
                continue
            if row["record_hash"] == current["record_hash"]:
                unchanged += 1
                continue
            current["is_current"] = False
            current["effective_to"] = row["effective_from"]
            history[row["customer_id"]].append(_open_version(row, batch.batch_id, open_ended))
            versioned += 1
        counts = input_counts(batch.extracted_rows, batch.accepted, batch.rejected)
        stats.append(
            BatchStats(
                batch_id=batch.batch_id,
                source_file=batch.source_file,
                extracted_rows=int(counts["extracted_rows"]),
                duplicate_rows=int(counts["duplicate_rows"]),
                accepted_rows=int(counts["accepted_rows"]),
                rejected_rows=int(counts["rejected_rows"]),
                inserted_rows=inserted,
                versioned_rows=versioned,
                unchanged_rows=unchanged,
                skipped_late_rows=skipped_late,
                accepted_checksum=str(counts["accepted_checksum"]),
            )
        )
    versions = [version for customer_versions in history.values() for version in customer_versions]
    versions.sort(key=lambda version: (version["customer_id"], version["effective_from"]))
    return versions, stats
