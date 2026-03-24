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
