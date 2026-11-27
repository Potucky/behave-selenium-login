"""Fixtures for the sample pipeline. Row builders live in tests/helpers.py."""

from __future__ import annotations

import pytest

from etl_validation.config import load_config
from etl_validation.extract import load_lookups
from etl_validation.pipeline import run_pipeline


@pytest.fixture
def lookups():
    return load_lookups(load_config())


@pytest.fixture
def config(tmp_path, monkeypatch):
    monkeypatch.setenv("ETL_TARGET_DB", str(tmp_path / "warehouse.duckdb"))
    monkeypatch.setenv("ETL_REPORT_DIR", str(tmp_path / "reports"))
    return load_config()


@pytest.fixture
def loaded(config):
    run_pipeline(config)
    return config
