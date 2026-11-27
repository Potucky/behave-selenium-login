"""Compare the DuckDB warehouse with the source-derived expectation."""

from __future__ import annotations

from collections import Counter
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb

from etl_validation.config import Config
from etl_validation.expected import fold
from etl_validation.extract import prepare_sources
from etl_validation.load import fetch_control, fetch_dimension, fetch_quarantine, query_rows
from etl_validation.models import BatchStats, CheckResult, Mismatch, ReconciliationReport
from etl_validation.transform import aggregate_checksum, format_credit, record_hash, warehouse_timestamp

ATTRIBUTE_COLUMNS = (
    "full_name",
    "email",
    "phone",
    "country_code",
    "country_name",
    "segment",
    "status",
    "credit_limit",
    "source_updated_at",
    "load_batch_id",
)
HISTORY_COLUMNS = ATTRIBUTE_COLUMNS + ("effective_to", "is_current", "record_hash")
CONTROL_FIELDS = (
    "source_file",
    "extracted_rows",
    "duplicate_rows",
    "accepted_rows",
    "rejected_rows",
    "inserted_rows",
    "versioned_rows",
    "unchanged_rows",
    "skipped_late_rows",
    "accepted_checksum",
)

_SQL_CHECKS: tuple[tuple[str, str, str], ...] = (
    (
        "scd_one_current",
        "Exactly one current row for each customer on the dimension.",
        """
        SELECT
            customer_id AS key,
            'is_current' AS column_name,
            '1' AS expected_value,
            CAST(SUM(CASE WHEN is_current THEN 1 ELSE 0 END) AS VARCHAR) AS actual_value
        FROM dim_customer
        GROUP BY customer_id
        HAVING SUM(CASE WHEN is_current THEN 1 ELSE 0 END) <> 1
        """,
    ),
    (
        "scd_no_overlap",
        "Effective ranges are half-open and do not overlap for a customer.",
        """
        SELECT
            a.customer_id AS key,
            'effective_range' AS column_name,
            'no overlap' AS expected_value,
            CAST(a.effective_from AS VARCHAR) || '..' || CAST(a.effective_to AS VARCHAR)
                || ' overlaps ' || CAST(b.effective_from AS VARCHAR) || '..'
                || CAST(b.effective_to AS VARCHAR) AS actual_value
        FROM dim_customer a
        JOIN dim_customer b
          ON a.customer_id = b.customer_id
         AND a.surrogate_key < b.surrogate_key
         AND a.effective_from < b.effective_to
         AND b.effective_from < a.effective_to
        """,
    ),
    (
        "scd_contiguous",
        "A closed version ends when the next version for that customer starts.",
        """
        WITH ordered AS (
            SELECT
                customer_id,
                effective_to,
                LEAD(effective_from) OVER (
                    PARTITION BY customer_id
                    ORDER BY effective_from, surrogate_key
                ) AS next_from
            FROM dim_customer
        )
        SELECT
            customer_id AS key,
            'effective_to' AS column_name,
            CAST(next_from AS VARCHAR) AS expected_value,
            CAST(effective_to AS VARCHAR) AS actual_value
        FROM ordered
        WHERE next_from IS NOT NULL
          AND effective_to <> next_from
        """,
    ),
    (
        "domain_values",
        "Segment, status, and country on the dimension exist in the lookup tables.",
        """
        SELECT customer_id AS key, 'segment' AS column_name,
               'lookup segment_name' AS expected_value, segment AS actual_value
        FROM dim_customer
        WHERE NOT EXISTS (
            SELECT 1 FROM ref_segment WHERE segment_name = dim_customer.segment
        )
        UNION ALL
        SELECT customer_id, 'status', 'lookup status_name', status
        FROM dim_customer
        WHERE NOT EXISTS (
            SELECT 1 FROM ref_status WHERE status_name = dim_customer.status
        )
        UNION ALL
        SELECT customer_id, 'country_code', 'lookup country_code', country_code
        FROM dim_customer
        WHERE NOT EXISTS (
            SELECT 1 FROM ref_country WHERE country_code = dim_customer.country_code
        )
        """,
    ),
    (
        "data_types",
        "Stored values match the email, phone, country, amount, and date contract.",
        """
        SELECT customer_id AS key, 'email' AS column_name,
               'lowercase email' AS expected_value, email AS actual_value
        FROM dim_customer
        WHERE NOT regexp_matches(email, '^[^@ ]+@[^@ ]+\\.[^@ ]+$')
        UNION ALL
        SELECT customer_id, 'phone', '10 to 15 digits', phone
        FROM dim_customer
        WHERE NOT regexp_matches(phone, '^[0-9]{10,15}$')
        UNION ALL
        SELECT customer_id, 'country_code', 'two uppercase letters', country_code
        FROM dim_customer
        WHERE NOT regexp_matches(country_code, '^[A-Z]{2}$')
        UNION ALL
        SELECT customer_id, 'credit_limit', 'non-negative DECIMAL(12,2)',
               typeof(credit_limit) || ' ' || CAST(credit_limit AS VARCHAR)
        FROM dim_customer
        WHERE typeof(credit_limit) <> 'DECIMAL(12,2)'
           OR credit_limit < 0
           OR credit_limit IS NULL
        UNION ALL
        SELECT customer_id, 'is_current', 'BOOLEAN', typeof(is_current)
        FROM dim_customer
        WHERE typeof(is_current) <> 'BOOLEAN'
        UNION ALL
        SELECT customer_id, 'effective_to', 'effective_from <= effective_to',
               CAST(effective_from AS VARCHAR) || '..' || CAST(effective_to AS VARCHAR)
        FROM dim_customer
        WHERE effective_from > effective_to
        """,
    ),
)


def build_report(config: Config) -> ReconciliationReport:
    if not config.target_db.exists():
        raise FileNotFoundError(
            f"Warehouse not found at {config.target_db}. Run `python -m etl_validation run` first."
        )
    _, batches = prepare_sources(config)
    expected_versions, expected_stats = fold(batches, config.open_ended)
    expected_rejects = _expected_rejects(batches)
    connection = duckdb.connect(str(config.target_db), read_only=True)
    try:
        actual_versions = fetch_dimension(connection)
        actual_rejects = fetch_quarantine(connection)
        actual_stats = fetch_control(connection)
        sql_checks = _sql_checks(connection)
    finally:
        connection.close()

    expected_current = [row for row in expected_versions if row["is_current"]]
    actual_current = [row for row in actual_versions if row["is_current"]]
    attribute_mismatches = _diff_by_customer(expected_current, actual_current, ATTRIBUTE_COLUMNS)
    history_mismatches = _diff_by_version(expected_versions, actual_versions, HISTORY_COLUMNS)
    hash_mismatches = _hash_mismatches(actual_versions)
    quarantine_mismatches = _diff_rejects(expected_rejects, actual_rejects)
    control_mismatches = _diff_control(expected_stats, actual_stats)
    checks = [
        _check(
            "current_row_count",
            "Open dimension rows equal the distinct customers in the source-derived current image.",
            len(expected_current),
            len(actual_current),
        ),
        _check(
            "history_row_count",
            "Dimension versions equal the source-derived SCD2 history, including closed rows.",
            len(expected_versions),
            len(actual_versions),
        ),
        _check(
            "current_checksum",
            "Aggregate checksum of current attributes, recomputed from the stored columns.",
            _current_checksum(expected_current),
            _current_checksum(actual_current),
        ),
        _check(
            "current_attributes",
            "Column-level comparison of the current customer image.",
            0,
            len(attribute_mismatches),
            attribute_mismatches,
        ),
        _check(
            "scd_history",
            "Every version's attributes, effective dates, and current flag match the expected history.",
            0,
            len(history_mismatches),
            history_mismatches,
        ),
        *sql_checks,
        _check(
            "record_hash_integrity",
            "The stored hash equals a fresh hash of the stored attributes.",
            0,
            len(hash_mismatches),
            hash_mismatches,
        ),
        _check(
            "quarantine",
            "Rejected source rows match the expected reason for each source line.",
            0,
            len(quarantine_mismatches),
            quarantine_mismatches,
        ),
        _check(
            "control_totals",
            "load_control counts and batch checksums match a replay of each source file.",
            0,
            len(control_mismatches),
            control_mismatches,
        ),
    ]
    return ReconciliationReport(
        generated_at=datetime.now(timezone.utc).replace(microsecond=0),
        target_db=_target_label(config.target_db),
        checks=tuple(checks),
    )


def _target_label(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(resolved)


def _check(
    check_id: str,
    description: str,
    expected: object,
    actual: object,
    mismatches: list[Mismatch] | None = None,
) -> CheckResult:
    found = tuple(_sorted(mismatches or []))
    return CheckResult(
        check_id=check_id,
        description=description,
        passed=str(expected) == str(actual) and not found,
        expected=str(expected),
        actual=str(actual),
        mismatches=found,
    )


def _sorted(mismatches: list[Mismatch]) -> list[Mismatch]:
    return sorted(mismatches, key=lambda item: (item.key, item.column, item.expected, item.actual))


def _display_date(value: object) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    return str(value)[:10]


def _display(column: str, value: object) -> str:
    if column == "credit_limit":
        return format_credit(value)
    if column == "source_updated_at":
        return warehouse_timestamp(value)
    if column in {"effective_from", "effective_to"}:
        return _display_date(value)
    if column == "is_current":
        return "true" if bool(value) else "false"
    if value is None:
        return ""
    return str(value)


def _current_checksum(rows: list[dict]) -> str:
    return aggregate_checksum([(str(row["customer_id"]), record_hash(row)) for row in rows])


def _duplicate_customers(rows: list[dict]) -> list[Mismatch]:
    seen: set[str] = set()
    mismatches: list[Mismatch] = []
    for row in rows:
        customer_id = str(row["customer_id"])
        if customer_id in seen:
            mismatches.append(
                Mismatch(key=customer_id, column="customer_id", expected="one current row", actual="duplicate")
            )
        seen.add(customer_id)
    return mismatches


def _diff_by_customer(expected_rows: list[dict], actual_rows: list[dict], columns: tuple[str, ...]) -> list[Mismatch]:
    mismatches = _duplicate_customers(expected_rows) + _duplicate_customers(actual_rows)
    expected_index = {str(row["customer_id"]): row for row in expected_rows}
    actual_index = {str(row["customer_id"]): row for row in actual_rows}
    for customer_id in sorted(set(expected_index) | set(actual_index)):
        expected = expected_index.get(customer_id)
        actual = actual_index.get(customer_id)
        mismatches.extend(_diff_pair(customer_id, expected, actual, columns))
    return mismatches


def _diff_by_version(expected_rows: list[dict], actual_rows: list[dict], columns: tuple[str, ...]) -> list[Mismatch]:
    def index(rows: list[dict]) -> tuple[dict[tuple[str, str], dict], list[Mismatch]]:
        indexed: dict[tuple[str, str], dict] = {}
        duplicates: list[Mismatch] = []
        for row in rows:
            key = (str(row["customer_id"]), _display_date(row["effective_from"]))
            if key in indexed:
                duplicates.append(
                    Mismatch(
                        key=key[0],
                        column="effective_from",
                        expected="one version on this date",
                        actual=f"duplicate {key[1]}",
                    )
                )
            indexed[key] = row
        return indexed, duplicates

    expected_index, expected_dups = index(expected_rows)
    actual_index, actual_dups = index(actual_rows)
    mismatches = expected_dups + actual_dups
    for key in sorted(set(expected_index) | set(actual_index)):
        label = f"{key[0]}@{key[1]}"
        mismatches.extend(_diff_pair(label, expected_index.get(key), actual_index.get(key), columns))
    return mismatches


def _diff_pair(
    label: str,
    expected: dict | None,
    actual: dict | None,
    columns: tuple[str, ...],
) -> list[Mismatch]:
    if expected is None:
        return [Mismatch(key=label, column="(row)", expected="missing", actual="present")]
    if actual is None:
        return [Mismatch(key=label, column="(row)", expected="present", actual="missing")]
    mismatches = []
    for column in columns:
        expected_value = _display(column, expected[column])
        actual_value = _display(column, actual[column])
        if expected_value != actual_value:
            mismatches.append(
                Mismatch(key=label, column=column, expected=expected_value, actual=actual_value)
            )
    return mismatches


def _hash_mismatches(rows: list[dict]) -> list[Mismatch]:
    mismatches = []
    for row in rows:
        recomputed = record_hash(row)
        if recomputed != row["record_hash"]:
            mismatches.append(
                Mismatch(
                    key=f"{row['customer_id']}@{_display_date(row['effective_from'])}",
                    column="record_hash",
                    expected=recomputed,
                    actual=str(row["record_hash"]),
                )
            )
    return mismatches


def _expected_rejects(batches) -> list[dict]:
    rejects = []
    for batch in batches:
        if batch.rejected.empty:
            continue
        for row in batch.rejected.to_dict(orient="records"):
            rejects.append(
                {
                    "source_file": row["source_file"],
                    "source_row_number": int(row["source_row_number"]),
                    "customer_id": row["customer_id"],
                    "reject_reason": row["reject_reason"],
                    "raw_payload": row["raw_payload"],
                }
            )
    return rejects


def _reject_identity(row: dict) -> tuple[str, int, str, str, str]:
    return (
        str(row["source_file"]),
        int(row["source_row_number"]),
        str(row["customer_id"]),
        str(row["reject_reason"]),
        str(row["raw_payload"]),
    )


def _diff_rejects(expected_rows: list[dict], actual_rows: list[dict]) -> list[Mismatch]:
    expected_counts = Counter(_reject_identity(row) for row in expected_rows)
    actual_counts = Counter(_reject_identity(row) for row in actual_rows)
    mismatches = []
    for identity in sorted(set(expected_counts) | set(actual_counts)):
        expected_count = expected_counts[identity]
        actual_count = actual_counts[identity]
        if expected_count == actual_count:
            continue
        source_file, source_row_number, customer_id, reason, _payload = identity
        mismatches.append(
            Mismatch(
                key=customer_id or "(blank)",
                column=reason,
                expected=f"{source_file}:{source_row_number} x{expected_count}",
                actual=f"{source_file}:{source_row_number} x{actual_count}",
            )
        )
    return mismatches


def _diff_control(expected_stats: list[BatchStats], actual_stats: list[BatchStats]) -> list[Mismatch]:
    expected_index = {item.batch_id: item for item in expected_stats}
    actual_index = {item.batch_id: item for item in actual_stats}
    mismatches = []
    for batch_id in sorted(set(expected_index) | set(actual_index)):
        expected = expected_index.get(batch_id)
        actual = actual_index.get(batch_id)
        if expected is None or actual is None:
            mismatches.append(
                Mismatch(
                    key=batch_id,
                    column="batch",
                    expected="present" if expected is not None else "missing",
                    actual="present" if actual is not None else "missing",
                )
            )
            continue
        for field in CONTROL_FIELDS:
            expected_value = str(getattr(expected, field))
            actual_value = str(getattr(actual, field))
            if expected_value != actual_value:
                mismatches.append(
                    Mismatch(key=batch_id, column=field, expected=expected_value, actual=actual_value)
                )
    return mismatches


def _sql_checks(connection: duckdb.DuckDBPyConnection) -> list[CheckResult]:
    checks = []
    for check_id, description, sql in _SQL_CHECKS:
        rows = query_rows(connection, sql)
        mismatches = [
            Mismatch(
                key=str(row["key"]),
                column=str(row["column_name"]),
                expected=str(row["expected_value"]),
                actual=str(row["actual_value"]),
            )
            for row in rows
        ]
        checks.append(_check(check_id, description, 0, len(mismatches), mismatches))
    return checks
