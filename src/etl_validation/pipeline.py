"""Rebuild the sample warehouse from the configured source files."""

from __future__ import annotations

import duckdb

from etl_validation.config import Config
from etl_validation.extract import prepare_sources
from etl_validation.load import create_schema, load_batch, load_references, reset_database
from etl_validation.models import BatchStats


def run_pipeline(config: Config) -> list[BatchStats]:
    """Drop the target database and apply every source file as its own SCD2 batch."""
    lookups, batches = prepare_sources(config)
    reset_database(config.target_db)
    connection = duckdb.connect(str(config.target_db))
    try:
        create_schema(connection)
        load_references(connection, lookups)
        return [load_batch(connection, batch, config.open_ended) for batch in batches]
    finally:
        connection.close()
