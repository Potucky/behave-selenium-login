"""Read the sample customer files and lookup tables."""

from __future__ import annotations

from typing import NamedTuple

import pandas as pd

from etl_validation.config import Config
from etl_validation.transform import Lookups, prepare_batch

SOURCE_COLUMNS = (
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


class PreparedBatch(NamedTuple):
    batch_id: str
    source_file: str
    extracted_rows: int
    accepted: pd.DataFrame
    rejected: pd.DataFrame


def _mapping(path, key: str, value: str) -> dict[str, str]:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [column for column in (key, value) if column not in frame.columns]
    if missing:
        raise ValueError(f"{path} is missing columns: {', '.join(missing)}")
    mapping: dict[str, str] = {}
    for record in frame.to_dict(orient="records"):
        code = str(record[key]).strip().upper()
        name = " ".join(str(record[value]).split())
        if not code or not name:
            raise ValueError(f"{path} has a blank {key} or {value}")
        if code in mapping:
            raise ValueError(f"{path} has a duplicate {key}: {code}")
        mapping[code] = name
    return mapping


def load_lookups(config: Config) -> Lookups:
    return Lookups(
        countries=_mapping(config.countries_path, "country_code", "country_name"),
        segments=_mapping(config.segments_path, "segment_code", "segment_name"),
        statuses=_mapping(config.statuses_path, "status_code", "status_name"),
    )


def read_customers(path) -> pd.DataFrame:
    frame = pd.read_csv(path, dtype=str, keep_default_na=False)
    missing = [column for column in SOURCE_COLUMNS if column not in frame.columns]
    if missing:
        raise ValueError(f"{path.name} is missing columns: {', '.join(missing)}")
    frame = frame.loc[:, list(SOURCE_COLUMNS)].copy()
    frame["source_file"] = path.name
    frame["source_row_number"] = range(1, len(frame) + 1)
    return frame


def prepare_sources(config: Config) -> tuple[Lookups, list[PreparedBatch]]:
    if not config.source_dir.is_dir():
        raise FileNotFoundError(f"Source directory does not exist: {config.source_dir}")
    files = config.source_files
    if not files:
        raise FileNotFoundError(
            f"No files matched {config.source_glob!r} in {config.source_dir}"
        )
    lookups = load_lookups(config)
    batches = []
    for path in files:
        raw = read_customers(path)
        accepted, rejected = prepare_batch(raw, lookups)
        batches.append(
            PreparedBatch(
                batch_id=path.stem,
                source_file=path.name,
                extracted_rows=len(raw),
                accepted=accepted,
                rejected=rejected,
            )
        )
    return lookups, batches
