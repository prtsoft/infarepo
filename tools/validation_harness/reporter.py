"""
Report writer for validation-harness.

Writes:
  validation-report.json      Full machine-readable report.
  validation-summary.txt      Human-readable text summary (console-friendly).

HIPAA safety: no data values appear in any report output — only counts,
percentages, table names, column names, and pass/fail status.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from .models import ValidationReport

log = logging.getLogger(__name__)

_PASS = "PASS"
_FAIL = "FAIL"


def _status(passed: bool) -> str:
    return _PASS if passed else _FAIL


def write_json_report(report: ValidationReport, output_dir: Path) -> Path:
    """Write the full JSON report."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / "validation-report.json"
    with open(out, "w", encoding="utf-8") as fh:
        json.dump(report.to_dict(), fh, indent=2, ensure_ascii=False)
    log.info("Written: %s", out)
    return out


def write_text_summary(report: ValidationReport, output_dir: Path) -> Path:
    """Write a human-readable text summary (HIPAA-safe)."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    out = output_dir / "validation-summary.txt"

    lines = []
    s = report.summary

    lines.append("=" * 70)
    lines.append("VALIDATION SUMMARY")
    lines.append(f"Generated : {report.generated}")
    lines.append(f"Config    : {report.config_path}")
    lines.append(f"Overall   : {_status(s.overall_passed)}")
    lines.append("=" * 70)

    # Recon
    lines.append(f"\nROW COUNT RECONCILIATION  ({s.recon_passed}/{s.recon_total} passed)")
    lines.append("-" * 70)
    for r in report.recon_results:
        badge = _status(r.passed)
        lines.append(
            f"  [{badge}]  {r.table_pair}"
        )
        lines.append(
            f"          source={r.source_count:,}  target={r.target_count:,}  "
            f"delta={r.delta:+,} ({r.delta_pct:.2f}%)  tolerance={r.tolerance_pct}%"
        )
        for note in r.notes:
            lines.append(f"          Note: {note}")

    # Schema diff
    lines.append(f"\nSCHEMA DIFF  ({s.schema_diff_passed}/{s.schema_diff_total} passed)")
    lines.append("-" * 70)
    for d in report.schema_diffs:
        badge = _status(d.passed)
        lines.append(f"  [{badge}]  {d.table_pair}")
        if d.missing_in_target:
            lines.append(f"          Missing in target : {', '.join(d.missing_in_target)}")
        if d.extra_in_target:
            lines.append(f"          Extra in target   : {', '.join(d.extra_in_target)}")
        for m in d.type_mismatches:
            compat = "compatible" if m.compatible else "INCOMPATIBLE"
            lines.append(
                f"          Type mismatch [{compat}]: "
                f"{m.column} — source={m.source_type} target={m.target_type}"
            )
        if d.ignored_columns:
            lines.append(f"          Ignored columns   : {', '.join(d.ignored_columns)}")

    # Rules
    lines.append(f"\nBUSINESS RULES  ({s.rules_passed}/{s.rules_total} passed)")
    lines.append("-" * 70)
    for r in report.rule_results:
        badge = _status(r.passed)
        hipaa = " [HIPAA]" if r.hipaa_flagged else ""
        col = f".{r.column}" if r.column else ""
        lines.append(
            f"  [{badge}]{hipaa}  {r.name}  ({r.rule_type})"
        )
        lines.append(
            f"          table={r.table}{col}  "
            f"failing={r.failing_count:,}/{r.total_count:,} ({r.fail_pct:.2f}%)"
        )

    # HIPAA notices
    if s.hipaa_flags:
        lines.append(f"\n  *** {s.hipaa_flags} rule(s) flagged HIPAA-sensitive column(s).")
        lines.append( "      Values are NOT logged. Review counts with your privacy officer.")

    lines.append("\n" + "=" * 70)
    overall = "ALL CHECKS PASSED" if s.overall_passed else "SOME CHECKS FAILED — review above"
    lines.append(f"  {overall}")
    lines.append("=" * 70)

    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info("Written: %s", out)
    return out


_HTML_TEMPLATE = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<title>Validation Sign-Off Report</title>
<style>
  body  { font-family: Arial, sans-serif; font-size: 13px; margin: 24px; color: #222; }
  h1    { font-size: 20px; margin-bottom: 4px; }
  h2    { font-size: 15px; margin-top: 24px; border-bottom: 1px solid #ccc; padding-bottom: 4px; }
  table { border-collapse: collapse; width: 100%%; margin-bottom: 16px; }
  th    { background: #344b66; color: #fff; padding: 6px 10px; text-align: left; font-size: 12px; }
  td    { padding: 5px 10px; border-bottom: 1px solid #eee; }
  tr:hover td { background: #f5f7fa; }
  .pass { color: #2e7d32; font-weight: bold; }
  .fail { color: #c62828; font-weight: bold; }
  .hipaa{ color: #e65100; font-weight: bold; }
  .badge-pass { background:#e8f5e9; color:#2e7d32; padding:1px 6px; border-radius:3px; font-weight:bold; }
  .badge-fail { background:#ffebee; color:#c62828; padding:1px 6px; border-radius:3px; font-weight:bold; }
  .section-overall-pass { background: #e8f5e9; padding: 8px 12px; border-left: 4px solid #2e7d32; }
  .section-overall-fail { background: #ffebee; padding: 8px 12px; border-left: 4px solid #c62828; }
  .signoff { margin-top: 40px; border: 1px solid #aaa; padding: 16px; max-width: 480px; }
  .signoff table td { border: none; padding: 4px 8px; }
  .signoff .line { border-bottom: 1px solid #555; min-width: 200px; display: inline-block; height: 18px; }
  .hipaa-notice { background:#fff3e0; border-left:4px solid #e65100; padding:8px 12px; margin:12px 0; font-size:12px; }
</style>
</head>
<body>
<h1>Migration Validation Sign-Off Report</h1>
<p>Generated: {generated}<br>Config: {config_path}</p>
<div class="{overall_class}">
  Overall result: <span class="{overall_text_class}">{overall_text}</span>
  &nbsp;|&nbsp; Recon: {recon_passed}/{recon_total}
  &nbsp;|&nbsp; Schema: {schema_passed}/{schema_total}
  &nbsp;|&nbsp; Rules: {rules_passed}/{rules_total}
</div>

{hipaa_notice}

<h2>Row Count Reconciliation</h2>
<table>
<tr><th>Status</th><th>Table Pair</th><th>Source Rows</th><th>Target Rows</th><th>Delta</th><th>Delta %</th><th>Tolerance</th><th>Notes</th></tr>
{recon_rows}
</table>

<h2>Schema Diff</h2>
<table>
<tr><th>Status</th><th>Table Pair</th><th>Missing in Target</th><th>Extra in Target</th><th>Type Mismatches</th><th>Ignored</th></tr>
{schema_rows}
</table>

<h2>Business Rules</h2>
<table>
<tr><th>Status</th><th>Rule</th><th>Type</th><th>Table</th><th>Column</th><th>Failing / Total</th><th>Fail %</th><th>HIPAA</th></tr>
{rule_rows}
</table>

<div class="signoff">
<h2 style="margin-top:0">Data Owner Sign-Off</h2>
<table>
<tr><td>Name:</td><td><span class="line"></span></td></tr>
<tr><td>Title:</td><td><span class="line"></span></td></tr>
<tr><td>Date:</td><td><span class="line"></span></td></tr>
<tr><td>Signature:</td><td><span class="line"></span></td></tr>
</table>
<p style="font-size:11px;color:#666">
  By signing, I confirm that the migration validation results above have been reviewed
  and that the data meets the agreed acceptance criteria.
</p>
</div>
</body>
</html>
"""


def _badge(passed: bool) -> str:
    if passed:
        return '<span class="badge-pass">PASS</span>'
    return '<span class="badge-fail">FAIL</span>'


def write_html_report(report: ValidationReport, output_path: Path) -> Path:
    """
    Write a self-contained HTML sign-off report.

    Includes: summary table, row count comparison, failed rules with SQL,
    and a printable signature block for the data owner.

    HIPAA-safe: no data values, only counts/percentages.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    s = report.summary

    # Recon rows
    recon_rows = []
    for r in report.recon_results:
        notes = "; ".join(r.notes) if r.notes else ""
        recon_rows.append(
            f"<tr><td>{_badge(r.passed)}</td>"
            f"<td>{r.table_pair}</td>"
            f"<td>{r.source_count:,}</td>"
            f"<td>{r.target_count:,}</td>"
            f"<td>{r.delta:+,}</td>"
            f"<td>{r.delta_pct:.2f}%</td>"
            f"<td>{r.tolerance_pct}%</td>"
            f"<td>{notes}</td></tr>"
        )

    # Schema rows
    schema_rows = []
    for d in report.schema_diffs:
        missing = ", ".join(d.missing_in_target) if d.missing_in_target else "—"
        extra   = ", ".join(d.extra_in_target)   if d.extra_in_target   else "—"
        mismatches = "; ".join(
            f"{m.column}: {m.source_type}→{m.target_type}" + (" ⚠" if not m.compatible else "")
            for m in d.type_mismatches
        ) or "—"
        ignored = ", ".join(d.ignored_columns) if d.ignored_columns else "—"
        schema_rows.append(
            f"<tr><td>{_badge(d.passed)}</td>"
            f"<td>{d.table_pair}</td>"
            f"<td>{missing}</td>"
            f"<td>{extra}</td>"
            f"<td>{mismatches}</td>"
            f"<td style='color:#888'>{ignored}</td></tr>"
        )

    # Rule rows
    rule_rows = []
    for r in report.rule_results:
        hipaa_cell = '<span class="hipaa">YES</span>' if r.hipaa_flagged else ""
        rule_rows.append(
            f"<tr><td>{_badge(r.passed)}</td>"
            f"<td>{r.name}</td>"
            f"<td>{r.rule_type}</td>"
            f"<td>{r.table}</td>"
            f"<td>{r.column or '—'}</td>"
            f"<td>{r.failing_count:,} / {r.total_count:,}</td>"
            f"<td>{r.fail_pct:.2f}%</td>"
            f"<td>{hipaa_cell}</td></tr>"
        )

    overall_class = "section-overall-pass" if s.overall_passed else "section-overall-fail"
    overall_text  = "ALL CHECKS PASSED" if s.overall_passed else "SOME CHECKS FAILED"
    overall_text_class = "pass" if s.overall_passed else "fail"

    hipaa_notice = ""
    if s.hipaa_flags:
        hipaa_notice = (
            f'<div class="hipaa-notice">'
            f'<strong>HIPAA Notice:</strong> {s.hipaa_flags} rule(s) involve PHI-sensitive columns. '
            f'Only counts are shown — no actual data values are logged or displayed. '
            f'Review with your privacy officer before distribution.'
            f'</div>'
        )

    html = _HTML_TEMPLATE.format(
        generated=report.generated,
        config_path=report.config_path,
        overall_class=overall_class,
        overall_text=overall_text,
        overall_text_class=overall_text_class,
        recon_passed=s.recon_passed,
        recon_total=s.recon_total,
        schema_passed=s.schema_diff_passed,
        schema_total=s.schema_diff_total,
        rules_passed=s.rules_passed,
        rules_total=s.rules_total,
        hipaa_notice=hipaa_notice,
        recon_rows="\n".join(recon_rows) or "<tr><td colspan=8>No recon results.</td></tr>",
        schema_rows="\n".join(schema_rows) or "<tr><td colspan=6>No schema diffs.</td></tr>",
        rule_rows="\n".join(rule_rows) or "<tr><td colspan=8>No rules evaluated.</td></tr>",
    )

    output_path.write_text(html, encoding="utf-8")
    log.info("Written: %s", output_path)
    return output_path


def print_summary(report: ValidationReport) -> None:
    """Print a coloured summary to stdout (falls back to plain text)."""
    try:
        from rich.console import Console
        from rich.table import Table
        from rich import box

        console = Console()
        s = report.summary
        overall_style = "green bold" if s.overall_passed else "red bold"
        console.print(f"\n[{overall_style}]{'PASS' if s.overall_passed else 'FAIL'}[/{overall_style}]"
                      f"  —  Validation report: {report.config_path}")

        # Recon table
        t = Table(box=box.SIMPLE_HEAD, header_style="bold magenta", title="Row Count Recon")
        t.add_column("Status", width=6)
        t.add_column("Table pair", style="cyan")
        t.add_column("Source", justify="right")
        t.add_column("Target", justify="right")
        t.add_column("Delta", justify="right")
        t.add_column("Delta %", justify="right")
        for r in report.recon_results:
            st = "green" if r.passed else "red"
            t.add_row(
                f"[{st}]{'OK' if r.passed else 'FAIL'}[/{st}]",
                r.table_pair,
                f"{r.source_count:,}",
                f"{r.target_count:,}",
                f"{r.delta:+,}",
                f"{r.delta_pct:.2f}%",
            )
        console.print(t)

        # Rules table
        if report.rule_results:
            t2 = Table(box=box.SIMPLE_HEAD, header_style="bold magenta", title="Business Rules")
            t2.add_column("Status", width=6)
            t2.add_column("Rule", style="white")
            t2.add_column("Type", style="yellow")
            t2.add_column("Table", style="cyan")
            t2.add_column("Column", style="green")
            t2.add_column("Failing", justify="right")
            t2.add_column("HIPAA")
            for r in report.rule_results:
                st = "green" if r.passed else "red"
                t2.add_row(
                    f"[{st}]{'OK' if r.passed else 'FAIL'}[/{st}]",
                    r.name, r.rule_type, r.table,
                    r.column or "",
                    f"{r.failing_count:,}/{r.total_count:,}",
                    "[red bold]YES[/red bold]" if r.hipaa_flagged else "",
                )
            console.print(t2)

        if s.hipaa_flags:
            console.print(
                f"\n[red bold]  {s.hipaa_flags} HIPAA-flagged rule(s)[/red bold] — "
                "counts only, no values logged."
            )

    except ImportError:
        # Fallback: print the text summary to stdout
        lines = write_text_summary.__doc__ or ""
        s = report.summary
        print(f"\n{'PASS' if s.overall_passed else 'FAIL'}  —  "
              f"{s.recon_passed}/{s.recon_total} recon  "
              f"{s.schema_diff_passed}/{s.schema_diff_total} schema  "
              f"{s.rules_passed}/{s.rules_total} rules")
        for r in report.recon_results:
            badge = "OK  " if r.passed else "FAIL"
            print(f"  [{badge}]  {r.table_pair}  {r.source_count:,}→{r.target_count:,}"
                  f"  delta={r.delta:+,} ({r.delta_pct:.2f}%)")
        for r in report.rule_results:
            badge = "OK  " if r.passed else "FAIL"
            hipaa = " [HIPAA]" if r.hipaa_flagged else ""
            print(f"  [{badge}]{hipaa}  {r.name}  {r.failing_count:,}/{r.total_count:,} failing")
