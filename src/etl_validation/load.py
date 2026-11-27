"""DuckDB schema and the SCD2 merge.

Effective dates are a daily grain. A closed version's effective_to equals the
next version's effective_from, and the range is half-open. A row whose
source_updated_at is not newer than the current version is ignored, even when
the attributes differ.
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import duckdb
import pandas as pd

from etl_validation.extract import PreparedBatch
from etl_validation.models import BatchStats
from etl_validation.transform import Lookups, input_counts, warehouse_timestamp

SCHEMA = """
CREATE SEQUENCE dim_customer_sk START 1;

CREATE TABLE dim_customer (
    surrogate_key BIGINT PRIMARY KEY,
    customer_id VARCHAR NOT NULL,
    full_name VARCHAR NOT NULL,
    email VARCHAR NOT NULL,
    phone VARCHAR NOT NULL,
    country_code VARCHAR NOT NULL,
    country_name VARCHAR NOT NULL,
    segment VARCHAR NOT NULL,
    status VARCHAR NOT NULL,
    credit_limit DECIMAL(12, 2) NOT NULL,
    source_updated_at TIMESTAMP NOT NULL,
    effective_from DATE NOT NULL,
    effective_to DATE NOT NULL,
    is_current BOOLEAN NOT NULL,
    record_hash VARCHAR NOT NULL,
    load_batch_id VARCHAR NOT NULL
);

CREATE TABLE quarantine (
    batch_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    source_row_number INTEGER NOT NULL,
    customer_id VARCHAR NOT NULL,
    reject_reason VARCHAR NOT NULL,
    raw_payload VARCHAR NOT NULL,
    rejected_at TIMESTAMP NOT NULL
);

CREATE TABLE load_control (
    batch_id VARCHAR NOT NULL,
    source_file VARCHAR NOT NULL,
    extracted_rows INTEGER NOT NULL,
    duplicate_rows INTEGER NOT NULL,
    accepted_rows INTEGER NOT NULL,
    rejected_rows INTEGER NOT NULL,
    inserted_rows INTEGER NOT NULL,
    versioned_rows INTEGER NOT NULL,
    unchanged_rows INTEGER NOT NULL,
    skipped_late_rows INTEGER NOT NULL,
    accepted_checksum VARCHAR NOT NULL,
    loaded_at TIMESTAMP NOT NULL
);

CREATE TABLE ref_country (
    country_code VARCHAR NOT NULL,
    country_name VARCHAR NOT NULL
);

CREATE TABLE ref_segment (
    segment_code VARCHAR NOT NULL,
    segment_name VARCHAR NOT NULL
);

CREATE TABLE ref_status (
    status_code VARCHAR NOT NULL,
    status_name VARCHAR NOT NULL
);
"""


def reset_database(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    for candidate in (path, Path(str(path) + ".wal")):
        if candidate.exists():
            candidate.unlink()


def create_schema(connection: duckdb.DuckDBPyConnection) -> None:
    for statement in SCHEMA.split(";"):
        statement = statement.strip()
        if statement:
            connection.execute(statement)


def load_references(connection: duckdb.DuckDBPyConnection, lookups: Lookups) -> None:
    _insert_reference(
        connection,
        "ref_country_input",
        "ref_country",
        ("country_code", "country_name"),
        lookups.countries,
    )
    _insert_reference(
        connection,
        "ref_segment_input",
        "ref_segment",
        ("segment_code", "segment_name"),
        lookups.segments,
    )
    _insert_reference(
        connection,
        "ref_status_input",
        "ref_status",
        ("status_code", "status_name"),
        lookups.statuses,
    )


def _insert_reference(
    connection: duckdb.DuckDBPyConnection,
    view_name: str,
    table_name: str,
    columns: tuple[str, str],
    mapping: dict[str, str],
) -> None:
    frame = pd.DataFrame(
        [{columns[0]: code, columns[1]: name} for code, name in sorted(mapping.items())]
    )
    connection.register(view_name, frame)
    connection.execute(
        f"INSERT INTO {table_name} SELECT {columns[0]}, {columns[1]} FROM {view_name}"
    )


def _stage_frame(accepted: pd.DataFrame) -> pd.DataFrame:
    stage = accepted.loc[
        :,
        [
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
            "record_hash",
        ],
    ].copy()
    stage["credit_limit"] = stage["credit_limit"].map(lambda value: f"{value:.2f}")
    stage["source_updated_at"] = stage["source_updated_at"].map(warehouse_timestamp)
    stage["effective_from"] = stage["effective_from"].map(lambda value: value.isoformat())
    return stage


def _merge_accepted(
    connection: duckdb.DuckDBPyConnection,
    accepted: pd.DataFrame,
    batch_id: str,
    open_ended: date,
) -> dict[str, int]:
    if accepted.empty:
        return {"insert": 0, "change": 0, "unchanged": 0, "skip_late": 0}

    connection.register("stg_customer", _stage_frame(accepted))
    connection.execute(
        """
        CREATE OR REPLACE TEMP TABLE classified AS
        SELECT
            s.*,
            d.surrogate_key AS current_sk,
            CASE
                WHEN d.customer_id IS NULL THEN 'insert'
                WHEN CAST(s.source_updated_at AS TIMESTAMP) <= d.source_updated_at THEN 'skip_late'
                WHEN s.record_hash = d.record_hash THEN 'unchanged'
                ELSE 'change'
            END AS action
        FROM stg_customer s
        LEFT JOIN dim_customer d
            ON d.customer_id = s.customer_id
           AND d.is_current
        """
    )
    connection.execute(
        """
        UPDATE dim_customer
        SET is_current = FALSE,
            effective_to = CAST(classified.effective_from AS DATE)
        FROM classified
        WHERE dim_customer.surrogate_key = classified.current_sk
          AND classified.action = 'change'
        """
    )
    connection.execute(
        """
        INSERT INTO dim_customer (
            surrogate_key, customer_id, full_name, email, phone, country_code,
            country_name, segment, status, credit_limit, source_updated_at,
            effective_from, effective_to, is_current, record_hash, load_batch_id
        )
        SELECT
            nextval('dim_customer_sk'),
            customer_id,
            full_name,
            email,
            phone,
            country_code,
            country_name,
            segment,
            status,
            CAST(credit_limit AS DECIMAL(12, 2)),
            CAST(source_updated_at AS TIMESTAMP),
            CAST(effective_from AS DATE),
            ?,
            TRUE,
            record_hash,
            ?
        FROM classified
        WHERE action IN ('insert', 'change')
        ORDER BY customer_id
        """,
        [open_ended, batch_id],
    )
    counts = dict(connection.execute("SELECT action, COUNT(*) FROM classified GROUP BY 1").fetchall())
    return {action: int(counts.get(action, 0)) for action in ("insert", "change", "unchanged", "skip_late")}


def _insert_rejects(
    connection: duckdb.DuckDBPyConnection,
    batch: PreparedBatch,
) -> None:
    if batch.rejected.empty:
        return
    connection.register("stg_rejected", batch.rejected)
    connection.execute(
        """
        INSERT INTO quarantine (
            batch_id, source_file, source_row_number, customer_id,
            reject_reason, raw_payload, rejected_at
        )
        SELECT ?, source_file, source_row_number, customer_id, reject_reason, raw_payload, CURRENT_TIMESTAMP
        FROM stg_rejected
        ORDER BY source_row_number
        """,
        [batch.batch_id],
    )


def load_batch(
    connection: duckdb.DuckDBPyConnection,
    batch: PreparedBatch,
    open_ended: date,
) -> BatchStats:
    counts = input_counts(batch.extracted_rows, batch.accepted, batch.rejected)
    actions = _merge_accepted(connection, batch.accepted, batch.batch_id, open_ended)
    _insert_rejects(connection, batch)
    stats = BatchStats(
        batch_id=batch.batch_id,
        source_file=batch.source_file,
        extracted_rows=int(counts["extracted_rows"]),
        duplicate_rows=int(counts["duplicate_rows"]),
        accepted_rows=int(counts["accepted_rows"]),
        rejected_rows=int(counts["rejected_rows"]),
        inserted_rows=actions["insert"],
        versioned_rows=actions["change"],
        unchanged_rows=actions["unchanged"],
        skipped_late_rows=actions["skip_late"],
        accepted_checksum=str(counts["accepted_checksum"]),
    )
    connection.execute(
        """
        INSERT INTO load_control VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
        """,
        [
            stats.batch_id,
            stats.source_file,
            stats.extracted_rows,
            stats.duplicate_rows,
            stats.accepted_rows,
            stats.rejected_rows,
            stats.inserted_rows,
            stats.versioned_rows,
            stats.unchanged_rows,
            stats.skipped_late_rows,
            stats.accepted_checksum,
        ],
    )
    return stats


def query_rows(connection: duckdb.DuckDBPyConnection, sql: str, params: list | None = None) -> list[dict]:
    cursor = connection.execute(sql, params or [])
    columns = [item[0] for item in cursor.description]
    return [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]


def fetch_dimension(connection: duckdb.DuckDBPyConnection) -> list[dict]:
    return query_rows(
        connection,
        """
        SELECT
            customer_id, full_name, email, phone, country_code, country_name,
            segment, status, credit_limit, source_updated_at, effective_from,
            effective_to, is_current, record_hash, load_batch_id
        FROM dim_customer
        ORDER BY customer_id, effective_from, surrogate_key
        """,
    )


def fetch_quarantine(connection: duckdb.DuckDBPyConnection) -> list[dict]:
    return query_rows(
        connection,
        """
        SELECT batch_id, source_file, source_row_number, customer_id, reject_reason, raw_payload
        FROM quarantine
        ORDER BY source_file, source_row_number
        """,
    )


def fetch_control(connection: duckdb.DuckDBPyConnection) -> list[BatchStats]:
    rows = query_rows(
        connection,
        """
        SELECT
            batch_id, source_file, extracted_rows, duplicate_rows, accepted_rows,
            rejected_rows, inserted_rows, versioned_rows, unchanged_rows,
            skipped_late_rows, accepted_checksum
        FROM load_control
        ORDER BY batch_id
        """,
    )
    return [BatchStats(**row) for row in rows]
