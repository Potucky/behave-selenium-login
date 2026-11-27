"""Write the reconciliation report as Markdown and a self-contained HTML file."""

from __future__ import annotations

import html
from pathlib import Path

from etl_validation.models import ReconciliationReport

_MAX_MISMATCH_ROWS = 100


def _cell(value: object) -> str:
    return str(value).replace("|", "\\|").replace("\n", " ")


def render_markdown(report: ReconciliationReport) -> str:
    status = "PASS" if report.passed else "FAIL"
    passed = sum(check.passed for check in report.checks)
    failed = len(report.checks) - passed
    lines = [
        "# Source-to-target reconciliation",
        "",
        f"- Result: **{status}**",
        f"- Generated at: {report.generated_at.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        f"- Target: `{report.target_db}`",
        f"- Checks: {passed} passed, {failed} failed",
        "",
        "| Check | Result | Expected | Actual | Mismatches |",
        "| --- | --- | --- | --- | ---: |",
    ]
    for check in report.checks:
        result = "PASS" if check.passed else "FAIL"
        lines.append(
            f"| `{check.check_id}` | {result} | {_cell(check.expected)} | {_cell(check.actual)} | {len(check.mismatches)} |"
        )
    lines.extend(["", "## Mismatches", ""])
    if report.passed:
        lines.append("None.")
        lines.append("")
        return "\n".join(lines)

    for check in report.failed_checks:
        lines.append(f"### `{check.check_id}`")
        lines.append("")
        lines.append(check.description)
        lines.append("")
        if not check.mismatches:
            lines.append(f"Expected `{_cell(check.expected)}`, actual `{_cell(check.actual)}`.")
            lines.append("")
            continue
        lines.extend(
            [
                "| Key | Column | Expected | Actual |",
                "| --- | --- | --- | --- |",
            ]
        )
        visible = check.mismatches[:_MAX_MISMATCH_ROWS]
        for item in visible:
            lines.append(
                f"| {_cell(item.key)} | {_cell(item.column)} | {_cell(item.expected)} | {_cell(item.actual)} |"
            )
        hidden = len(check.mismatches) - len(visible)
        if hidden:
            lines.append(f"| … | | | {hidden} more |")
        lines.append("")
    return "\n".join(lines)


def render_html(report: ReconciliationReport) -> str:
    status = "PASS" if report.passed else "FAIL"
    status_class = "pass" if report.passed else "fail"
    passed = sum(check.passed for check in report.checks)
    failed = len(report.checks) - passed
    rows = []
    for check in report.checks:
        result = "PASS" if check.passed else "FAIL"
        result_class = "pass" if check.passed else "fail"
        rows.append(
            "<tr>"
            f"<td><code>{html.escape(check.check_id)}</code>"
            f"<div class=\"desc\">{html.escape(check.description)}</div></td>"
            f"<td><span class=\"pill {result_class}\">{result}</span></td>"
            f"<td><code>{html.escape(check.expected)}</code></td>"
            f"<td><code>{html.escape(check.actual)}</code></td>"
            f"<td class=\"num\">{len(check.mismatches)}</td>"
            "</tr>"
        )
    mismatch_blocks = []
    if report.passed:
        mismatch_blocks.append("<p class=\"none\">None.</p>")
    for check in report.failed_checks:
        body = [f"<h3><code>{html.escape(check.check_id)}</code></h3>", f"<p>{html.escape(check.description)}</p>"]
        if not check.mismatches:
            body.append(
                "<p>Expected <code>"
                f"{html.escape(check.expected)}</code>, actual <code>{html.escape(check.actual)}</code>.</p>"
            )
        else:
            body.append("<table><thead><tr><th>Key</th><th>Column</th><th>Expected</th><th>Actual</th></tr></thead><tbody>")
            for item in check.mismatches[:_MAX_MISMATCH_ROWS]:
                body.append(
                    "<tr>"
                    f"<td>{html.escape(item.key)}</td>"
                    f"<td>{html.escape(item.column)}</td>"
                    f"<td><code>{html.escape(item.expected)}</code></td>"
                    f"<td><code>{html.escape(item.actual)}</code></td>"
                    "</tr>"
                )
            hidden = len(check.mismatches) - _MAX_MISMATCH_ROWS
            if hidden > 0:
                body.append(f"<tr><td colspan=\"4\">{hidden} more</td></tr>")
            body.append("</tbody></table>")
        mismatch_blocks.append("".join(body))

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Source-to-target reconciliation</title>
  <style>
    :root {{ color-scheme: light; }}
    body {{ margin: 2rem auto; max-width: 1100px; padding: 0 1.25rem;
            font-family: "Segoe UI", Helvetica, Arial, sans-serif; color: #1f2933; }}
    h1 {{ font-size: 1.7rem; margin-bottom: 0.25rem; }}
    h2 {{ margin-top: 2rem; font-size: 1.2rem; }}
    .meta {{ color: #52606d; margin-top: 0; }}
    .banner {{ display: flex; gap: 1rem; align-items: center; margin: 1.25rem 0; }}
    .pill {{ display: inline-block; padding: 0.2rem 0.6rem; border-radius: 999px;
             font-size: 0.85rem; font-weight: 700; letter-spacing: 0.03em; }}
    .pass {{ background: #e3f9e5; color: #0f7b32; }}
    .fail {{ background: #fde8e8; color: #b42318; }}
    .summary {{ color: #52606d; }}
    table {{ border-collapse: collapse; width: 100%; margin: 0.5rem 0 1.5rem; }}
    th, td {{ border-bottom: 1px solid #e4e7eb; text-align: left; padding: 0.55rem 0.45rem;
              vertical-align: top; font-size: 0.95rem; }}
    th {{ background: #f5f7fa; }}
    td.num, th:last-child {{ text-align: right; }}
    code {{ font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; font-size: 0.86rem;
            word-break: break-all; }}
    .desc {{ color: #52606d; margin-top: 0.2rem; font-size: 0.85rem; }}
    .none {{ color: #0f7b32; font-weight: 600; }}
    footer {{ color: #7b8794; font-size: 0.85rem; margin-top: 2rem; }}
  </style>
</head>
<body>
  <h1>Source-to-target reconciliation</h1>
  <p class="meta">Synthetic customer dimension. Portfolio demo, not a production warehouse.</p>
  <div class="banner">
    <span class="pill {status_class}">{status}</span>
    <span class="summary">{passed} passed, {failed} failed</span>
  </div>
  <p class="meta">Generated at {html.escape(report.generated_at.strftime("%Y-%m-%d %H:%M:%S UTC"))}<br>
  Target <code>{html.escape(report.target_db)}</code></p>
  <table>
    <thead>
      <tr><th>Check</th><th>Result</th><th>Expected</th><th>Actual</th><th>Mismatches</th></tr>
    </thead>
    <tbody>
      {"".join(f"{row}\n" for row in rows)}
    </tbody>
  </table>
  <h2>Mismatches</h2>
  {"".join(mismatch_blocks)}
  <footer>Generated by etl_validation. Sample data only.</footer>
</body>
</html>
"""


def write_reports(report: ReconciliationReport, report_dir: Path) -> tuple[Path, Path]:
    report_dir.mkdir(parents=True, exist_ok=True)
    markdown_path = report_dir / "reconciliation.md"
    html_path = report_dir / "reconciliation.html"
    markdown_path.write_text(render_markdown(report), encoding="utf-8")
    html_path.write_text(render_html(report), encoding="utf-8")
    return markdown_path, html_path
