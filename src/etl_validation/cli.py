"""Command line entry points for the sample load and the reconciliation report."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from etl_validation.config import load_config
from etl_validation.pipeline import run_pipeline
from etl_validation.reconcile import build_report
from etl_validation.report import render_markdown, write_reports


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="etl_validation",
        description="Load the sample customer files and reconcile the warehouse to the source.",
    )
    parser.add_argument("--config", type=Path, help="YAML config path. Overrides ETL_CONFIG.")
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("run", help="Rebuild the DuckDB warehouse from the source files")
    commands.add_parser("reconcile", help="Compare the warehouse to the source and write a report")
    args = parser.parse_args(argv)
    if args.config is not None:
        os.environ["ETL_CONFIG"] = str(args.config)

    config = load_config()
    if args.command == "run":
        for item in run_pipeline(config):
            print(
                f"{item.source_file}: extracted={item.extracted_rows} "
                f"accepted={item.accepted_rows} rejected={item.rejected_rows} "
                f"duplicates={item.duplicate_rows} inserted={item.inserted_rows} "
                f"versioned={item.versioned_rows} unchanged={item.unchanged_rows} "
                f"skipped_late={item.skipped_late_rows}"
            )
        return 0

    report = build_report(config)
    markdown_path, html_path = write_reports(report, config.report_dir)
    sys.stdout.write(render_markdown(report))
    print(f"Wrote {markdown_path}", file=sys.stderr)
    print(f"Wrote {html_path}", file=sys.stderr)
    return 0 if report.passed else 1
