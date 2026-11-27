"""The Markdown and HTML reconciliation reports, and the command line."""

from __future__ import annotations

import duckdb
import pytest

from etl_validation.cli import main
from etl_validation.reconcile import build_report
from etl_validation.report import render_html, render_markdown, write_reports

pytestmark = pytest.mark.integration


def test_passing_report_lists_every_check_and_no_mismatches(loaded):
    report = build_report(loaded)
    markdown = render_markdown(report)
    html = render_html(report)
    assert "- Result: **PASS**" in markdown
    assert "None." in markdown
    assert "current_checksum" in markdown
    assert "FAIL" not in markdown
    assert "<span class=\"pill pass\">PASS</span>" in html
    assert "Synthetic customer dimension" in html
    for check in report.checks:
        assert check.check_id in html


def test_failing_report_names_the_mismatched_customer(loaded, tmp_path):
    connection = duckdb.connect(str(loaded.target_db))
    connection.execute(
        """
        UPDATE dim_customer
        SET email = 'tampered@example.com'
        WHERE customer_id = 'C003' AND is_current
        """
    )
    connection.execute("CHECKPOINT")
    connection.close()

    report = build_report(loaded)
    markdown_path, html_path = write_reports(report, tmp_path / "reports")
    markdown = markdown_path.read_text(encoding="utf-8")
    html = html_path.read_text(encoding="utf-8")
    assert "- Result: **FAIL**" in markdown
    assert "C003" in markdown
    assert "tampered@example.com" in markdown
    assert "current_attributes" in markdown
    assert "tampered@example.com" in html
    assert "<span class=\"pill fail\">FAIL</span>" in html


def test_cli_rebuild_and_reconcile_exits_zero(config, capsys):
    assert main(["run"]) == 0
    run_output = capsys.readouterr().out
    assert "customers_20260115.csv: extracted=13 accepted=7 rejected=5" in run_output
    assert "customers_20260122.csv: extracted=8 accepted=6 rejected=1" in run_output

    assert main(["reconcile"]) == 0
    report_output = capsys.readouterr().out
    assert "- Result: **PASS**" in report_output
    assert (config.report_dir / "reconciliation.md").exists()
    assert (config.report_dir / "reconciliation.html").exists()


def test_target_database_path_can_be_overridden_from_the_environment(tmp_path, monkeypatch):
    database = tmp_path / "override.duckdb"
    monkeypatch.setenv("ETL_TARGET_DB", str(database))
    from etl_validation.config import load_config

    config = load_config()
    assert config.target_db == database.resolve()
    assert main(["run"]) == 0
    assert database.exists()
