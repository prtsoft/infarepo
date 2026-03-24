"""
validation-harness CLI

Commands:
  validate      Run full validation suite (recon + schema diff + rules).
  recon         Row count reconciliation only.
  diff-schema   Schema diff only.
  report        Pretty-print an existing validation-report.json.
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import click

from . import __version__
from .runner import run_validation
from .reporter import write_json_report, write_text_summary, print_summary


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        format="%(asctime)s  %(levelname)-8s  %(message)s",
        datefmt="%H:%M:%S",
        level=level,
        stream=sys.stderr,
    )


@click.group()
@click.version_option(__version__)
def cli():
    """
    validation-harness — Post-migration data validation for PC → Glue/Databricks.

    Runs row count reconciliation, schema diffs, and business rule assertions
    between source and target databases. HIPAA-safe: only counts are logged,
    never data values.
    """


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("config", type=click.Path(exists=True, path_type=Path))
@click.option("--output-dir", "-o", default="output/validation",
              type=click.Path(path_type=Path), show_default=True)
@click.option("--dry-run", is_flag=True, default=False,
              help="Parse config and show plan but do not execute SQL.")
@click.option("--verbose", "-v", is_flag=True, default=False)
def validate(config, output_dir, dry_run, verbose):
    """
    Run the full validation suite: recon + schema diff + business rules.

    CONFIG is a YAML file describing the source/target connections and
    the validation rules to apply.

    Example:

      validation-harness validate config/sales_mart_validation.yaml

      validation-harness validate config/sales_mart_validation.yaml --dry-run
    """
    _setup_logging(verbose)

    if dry_run:
        click.echo("  [dry-run] Parsing config without executing SQL...")

    report = run_validation(config, dry_run=dry_run)
    s = report.summary

    json_path = write_json_report(report, output_dir)
    txt_path  = write_text_summary(report, output_dir)

    click.echo()
    click.echo("  Output:")
    click.echo(f"    {json_path}")
    click.echo(f"    {txt_path}")
    click.echo()

    overall = "PASS" if s.overall_passed else "FAIL"
    click.echo(
        f"  Overall: {overall}  —  "
        f"Recon {s.recon_passed}/{s.recon_total}  "
        f"Schema {s.schema_diff_passed}/{s.schema_diff_total}  "
        f"Rules {s.rules_passed}/{s.rules_total}"
    )

    if s.hipaa_flags:
        click.echo(
            f"\n  {s.hipaa_flags} HIPAA-flagged rule(s) — "
            "review with your privacy officer before sharing the report.",
            err=True,
        )

    if not s.overall_passed:
        sys.exit(1)


# ---------------------------------------------------------------------------
# recon
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("config", type=click.Path(exists=True, path_type=Path))
@click.option("--output-dir", "-o", default="output/validation",
              type=click.Path(path_type=Path), show_default=True)
@click.option("--verbose", "-v", is_flag=True, default=False)
def recon(config, output_dir, verbose):
    """
    Row count reconciliation only (faster than full validate).

    Example:

      validation-harness recon config/sales_mart_validation.yaml
    """
    _setup_logging(verbose)
    report = run_validation(config, run_recon_only=True)
    s = report.summary

    write_json_report(report, output_dir)

    for r in report.recon_results:
        badge = "PASS" if r.passed else "FAIL"
        click.echo(
            f"  [{badge}]  {r.table_pair:<50}  "
            f"src={r.source_count:>10,}  tgt={r.target_count:>10,}  "
            f"delta={r.delta:>+8,} ({r.delta_pct:.2f}%)"
        )

    click.echo(f"\n  {s.recon_passed}/{s.recon_total} table(s) within tolerance.")
    if not s.overall_passed:
        sys.exit(1)


# ---------------------------------------------------------------------------
# diff-schema
# ---------------------------------------------------------------------------

@cli.command("diff-schema")
@click.argument("config", type=click.Path(exists=True, path_type=Path))
@click.option("--output-dir", "-o", default="output/validation",
              type=click.Path(path_type=Path), show_default=True)
@click.option("--verbose", "-v", is_flag=True, default=False)
def diff_schema(config, output_dir, verbose):
    """
    Schema diff only — compare column names and types between source and target.

    Example:

      validation-harness diff-schema config/sales_mart_validation.yaml
    """
    _setup_logging(verbose)
    report = run_validation(config, run_schema_only=True)
    s = report.summary

    write_json_report(report, output_dir)

    for d in report.schema_diffs:
        badge = "PASS" if d.passed else "FAIL"
        click.echo(f"  [{badge}]  {d.table_pair}")
        if d.missing_in_target:
            click.echo(f"    Missing in target: {', '.join(d.missing_in_target)}")
        if d.extra_in_target:
            click.echo(f"    Extra in target:   {', '.join(d.extra_in_target)}")
        for m in d.type_mismatches:
            compat = "ok" if m.compatible else "INCOMPATIBLE"
            click.echo(
                f"    Type [{compat}]: {m.column} — "
                f"source={m.source_type} target={m.target_type}"
            )

    click.echo(f"\n  {s.schema_diff_passed}/{s.schema_diff_total} schema(s) passed.")
    if not s.overall_passed:
        sys.exit(1)


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("report_json", type=click.Path(exists=True, path_type=Path))
def report(report_json):
    """
    Pretty-print an existing validation-report.json.

    Example:

      validation-harness report output/validation/validation-report.json
    """
    with open(report_json, encoding="utf-8") as fh:
        data = json.load(fh)

    # Re-hydrate summary for print
    s_data = data.get("summary", {})
    from .models import ValidationSummary, ValidationReport, ReconResult, SchemaDiff, RuleResult, SchemaDiff, TypeMismatch, SchemaField

    summary = ValidationSummary(
        files_validated=s_data.get("files_validated", 0),
        recon_total=s_data.get("recon_total", 0),
        recon_passed=s_data.get("recon_passed", 0),
        schema_diff_total=s_data.get("schema_diff_total", 0),
        schema_diff_passed=s_data.get("schema_diff_passed", 0),
        rules_total=s_data.get("rules_total", 0),
        rules_passed=s_data.get("rules_passed", 0),
        hipaa_flags=s_data.get("hipaa_flags", 0),
        overall_passed=s_data.get("overall_passed", False),
    )

    recon_results = [
        ReconResult(**{k: v for k, v in r.items()})
        for r in data.get("recon", [])
    ]
    rule_results = [
        RuleResult(**{k: v for k, v in r.items()})
        for r in data.get("rules", [])
    ]
    schema_diffs = []
    for d in data.get("schema_diffs", []):
        schema_diffs.append(SchemaDiff(
            table_pair=d["table_pair"],
            source_table=d.get("source_table", ""),
            target_table=d.get("target_table", ""),
            source_columns=[],
            target_columns=[],
            missing_in_target=d.get("missing_in_target", []),
            extra_in_target=d.get("extra_in_target", []),
            type_mismatches=[
                TypeMismatch(**m) for m in d.get("type_mismatches", [])
            ],
            ignored_columns=d.get("ignored_columns", []),
            passed=d.get("passed", False),
        ))

    rpt = ValidationReport(
        generated=data.get("_generated", ""),
        source_dsn=data.get("source_dsn", ""),
        target_dsn=data.get("target_dsn", ""),
        config_path=data.get("config_path", ""),
        recon_results=recon_results,
        schema_diffs=schema_diffs,
        rule_results=rule_results,
        summary=summary,
    )
    print_summary(rpt)


def main():
    cli()


if __name__ == "__main__":
    main()
