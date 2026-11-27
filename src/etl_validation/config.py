"""Load pipeline settings from YAML, with optional environment overrides."""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import date
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = REPO_ROOT / "config" / "pipeline.yaml"


@dataclass(frozen=True)
class Config:
    source_dir: Path
    source_glob: str
    countries_path: Path
    segments_path: Path
    statuses_path: Path
    target_db: Path
    report_dir: Path
    open_ended: date

    @property
    def source_files(self) -> list[Path]:
        """Source files in load order. Lexical order is the batch order."""
        return sorted(self.source_dir.glob(self.source_glob))


def _resolve(root: Path, value: str) -> Path:
    path = Path(value)
    if path.is_absolute():
        return path
    return (root / path).resolve()


def load_config(path: Path | None = None) -> Config:
    """Read config/pipeline.yaml unless ETL_CONFIG or an explicit path is set.

    ETL_SOURCE_DIR, ETL_TARGET_DB, and ETL_REPORT_DIR replace the matching
    YAML values. Relative override paths are resolved from the repository root.
    """
    config_path = Path(os.environ["ETL_CONFIG"]) if path is None and os.environ.get("ETL_CONFIG") else path
    config_path = (config_path or DEFAULT_CONFIG).resolve()
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    root = config_path.parent.parent if config_path.parent.name == "config" else config_path.parent

    open_ended = date.fromisoformat(str(raw.get("open_ended_date", "9999-12-31")))
    lookups = raw["lookups"]
    return Config(
        source_dir=_resolve(root, os.environ.get("ETL_SOURCE_DIR", raw["source_dir"])),
        source_glob=str(raw.get("source_glob", "customers_*.csv")),
        countries_path=_resolve(root, lookups["countries"]),
        segments_path=_resolve(root, lookups["segments"]),
        statuses_path=_resolve(root, lookups["statuses"]),
        target_db=_resolve(root, os.environ.get("ETL_TARGET_DB", raw["target_db"])),
        report_dir=_resolve(root, os.environ.get("ETL_REPORT_DIR", raw["report_dir"])),
        open_ended=open_ended,
    )
