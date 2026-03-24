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


# ---------------------------------------------------------------------------
# sign-off  (Phase 5E)
# ---------------------------------------------------------------------------

@cli.command("sign-off")
@click.argument("report_json", type=click.Path(exists=True, path_type=Path))
@click.option("--output", "-o", default="output/validation/sign-off-report.html",
              type=click.Path(path_type=Path), show_default=True,
              help="Path for the HTML sign-off report.")
def sign_off(report_json, output):
    """
    Generate a printable HTML sign-off report from a validation-report.json.

    Includes row count comparison, rule results, and a signature block
    for the data owner to sign before migration go-live.

    Example:

      validation-harness sign-off output/validation/validation-report.json
      validation-harness sign-off validation-report.json --output report.html
    """
    from .reporter import write_html_report
    from .models import (
        ValidationSummary, ValidationReport, ReconResult, SchemaDiff,
        RuleResult, TypeMismatch,
    )

    with open(report_json, encoding="utf-8") as fh:
        data = json.load(fh)

    s_data = data.get("summary", {})
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
    recon_results = [ReconResult(**r) for r in data.get("recon", [])]
    rule_results  = [RuleResult(**r)  for r in data.get("rules", [])]
    schema_diffs  = [
        SchemaDiff(
            table_pair=d["table_pair"],
            source_table=d.get("source_table", ""),
            target_table=d.get("target_table", ""),
            source_columns=[],
            target_columns=[],
            missing_in_target=d.get("missing_in_target", []),
            extra_in_target=d.get("extra_in_target", []),
            type_mismatches=[TypeMismatch(**m) for m in d.get("type_mismatches", [])],
            ignored_columns=d.get("ignored_columns", []),
            passed=d.get("passed", False),
        )
        for d in data.get("schema_diffs", [])
    ]
    rpt = ValidationReport(
        generated=data.get("_generated", ""),
        source_dsn=data.get("source_dsn", ""),
        target_dsn=data.get("target_dsn", ""),
        config_path=data.get("config_path", str(report_json)),
        recon_results=recon_results,
        schema_diffs=schema_diffs,
        rule_results=rule_results,
        summary=summary,
    )

    out_path = write_html_report(rpt, Path(output))
    click.echo(f"  Sign-off report → {out_path}")


# ---------------------------------------------------------------------------
# config-gen  (Phase 5C)
# ---------------------------------------------------------------------------

@cli.command("config-gen")
@click.argument("manifest_json", type=click.Path(exists=True, path_type=Path))
@click.argument("folder")
@click.argument("mapping")
@click.option("--source-dsn", default="", help="Source database DSN / connection string.")
@click.option("--target-dsn", default="", help="Target lakehouse DSN / connection string.")
@click.option("--output", "-o", default=None, type=click.Path(path_type=Path),
              help="Output path for the generated YAML config. Default: <folder>_<mapping>_validation.yaml")
@click.option("--tolerance-pct", default=0.5, show_default=True, type=float,
              help="Row count tolerance percentage.")
@click.option("--verbose", "-v", is_flag=True, default=False)
def config_gen(manifest_json, folder, mapping, source_dsn, target_dsn, output, tolerance_pct, verbose):
    """
    Auto-generate a validation config YAML from field-level lineage.

    Traces the field-level lineage for FOLDER/MAPPING and generates a
    validation config with recon, schema diff, and rule entries for each
    source→target table pair found.

    Example:

      validation-harness config-gen manifest.json SALES_MART M_LOAD_FACT_ORDERS \\
          --source-dsn "mssql+pyodbc://..." --target-dsn "databricks://..."
    """
    _setup_logging(verbose)

    import json as _json
    with open(manifest_json, encoding="utf-8") as fh:
        data = _json.load(fh)

    # Load manifest using pc_extractor
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parent.parent))
    from pc_extractor.lineage import trace_mapping
    from pc_extractor.xml_parser import _reconstruct_manifest  # type: ignore

    # Use the manifest loader from glue_gen (handles JSON reconstruction)
    try:
        from glue_gen.cli import _load_manifest
        manifest = _load_manifest(data)
    except Exception:
        click.echo("Error: could not load manifest. Ensure the JSON is a valid migration-manifest.json.", err=True)
        sys.exit(1)

    folder_obj = manifest.folders.get(folder)
    if not folder_obj:
        click.echo(f"Error: folder '{folder}' not found.", err=True)
        sys.exit(1)
    mapping_obj = folder_obj.mappings.get(mapping)
    if not mapping_obj:
        click.echo(f"Error: mapping '{mapping}' not found in '{folder}'.", err=True)
        sys.exit(1)

    lineage = trace_mapping(mapping_obj, folder_obj)

    from .config_gen import generate_validation_config, write_validation_config
    config = generate_validation_config(lineage, source_dsn, target_dsn, tolerance_pct)

    out = output or Path(f"{folder}_{mapping}_validation.yaml")
    write_validation_config(config, Path(out))
    click.echo(f"  Validation config → {out}")
    click.echo(f"  Tables: {len(config.get('validations', []))} source→target pair(s)")


# ---------------------------------------------------------------------------
# test-connection  (Phase 5F)
# ---------------------------------------------------------------------------

@cli.command("test-connection")
@click.argument("config", type=click.Path(exists=True, path_type=Path))
@click.option("--verbose", "-v", is_flag=True, default=False)
def test_connection(config, verbose):
    """
    Test source and target connections defined in a validation config YAML.

    Attempts to open both connections, runs SELECT 1, and reports latency.

    Example:

      validation-harness test-connection config/sales_mart_validation.yaml
    """
    _setup_logging(verbose)
    import time

    from .runner import load_config
    from .connections import open_connection

    cfg = load_config(Path(config))
    connections = cfg.get("connections", {})

    all_ok = True
    for conn_name, conn_cfg in connections.items():
        dsn = conn_cfg.get("dsn", "")
        if not dsn:
            click.echo(f"  [{conn_name}] No DSN configured — skipping")
            continue

        click.echo(f"  Testing [{conn_name}] {dsn[:60]}{'...' if len(dsn) > 60 else ''} ...")
        t0 = time.monotonic()
        try:
            conn = open_connection(dsn)
            conn.execute("SELECT 1").fetchone()
            elapsed_ms = (time.monotonic() - t0) * 1000
            click.echo(f"    OK  ({elapsed_ms:.0f}ms)")
        except Exception as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000
            click.echo(f"    FAIL ({elapsed_ms:.0f}ms): {exc}", err=True)
            all_ok = False

    if not all_ok:
        sys.exit(1)


def main():
    cli()


if __name__ == "__main__":
    main()
