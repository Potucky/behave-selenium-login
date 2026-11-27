"""Shared result types for the load and the reconciliation report."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


@dataclass(frozen=True)
class BatchStats:
    """Control totals for one source file after it has been applied."""

    batch_id: str
    source_file: str
    extracted_rows: int
    duplicate_rows: int
    accepted_rows: int
    rejected_rows: int
    inserted_rows: int
    versioned_rows: int
    unchanged_rows: int
    skipped_late_rows: int
    accepted_checksum: str


@dataclass(frozen=True)
class Mismatch:
    """One row/column (or control-total) difference."""

    key: str
    column: str
    expected: str
    actual: str


@dataclass(frozen=True)
class CheckResult:
    check_id: str
    description: str
    passed: bool
    expected: str
    actual: str
    mismatches: tuple[Mismatch, ...] = ()


@dataclass(frozen=True)
class ReconciliationReport:
    generated_at: datetime
    target_db: str
    checks: tuple[CheckResult, ...]

    @property
    def passed(self) -> bool:
        return all(check.passed for check in self.checks)

    @property
    def failed_checks(self) -> tuple[CheckResult, ...]:
        return tuple(check for check in self.checks if not check.passed)
