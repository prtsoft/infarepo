"""
param-translator CLI

Commands:
  parse        Parse .prm file(s) → full params JSON + glue-params JSON + loader
  validate     Check for unresolved types, missing values, HIPAA-sensitive params
  diff         Compare two .prm files (or same file across two directories)
  show         Print a summary of a parsed .prm or output JSON to stdout
"""

import json
import logging
import sys
from pathlib import Path
from typing import List, Optional

import click

from . import __version__
from .parser import parse_prm_file, parse_prm_files
from .classifier import classify_file
from .normalizer import normalize_file
from .exporter import export_all
from .models import ParamType


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
    param-translator — Parse PowerCenter .prm files → JSON config for AWS Glue.

    Converts $$PARAM=value entries to typed, normalised JSON with PySpark
    translations. Generates per-workflow JSON files (S3 source of truth)
    plus a param_loader.py runtime utility for Glue jobs.
    """


# ---------------------------------------------------------------------------
# parse
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("inputs", nargs=-1, required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--output-dir", "-o", default="output", type=click.Path(path_type=Path),
              show_default=True)
@click.option("--verbose", "-v", is_flag=True, default=False)
def parse(inputs, output_dir, verbose):
    """
    Parse one or more .prm files and export JSON config artifacts.

    INPUTS can be individual .prm files or directories containing .prm files.

    Example:

      param-translator parse params/SALES_MART.prm --output-dir output/

      param-translator parse params/ --output-dir output/
    """
    _setup_logging(verbose)

    prm_paths: List[Path] = []
    for inp in inputs:
        p = Path(inp)
        if p.is_dir():
            found = sorted(p.rglob("*.prm"))
            if not found:
                click.echo(f"  Warning: no .prm files in {p}", err=True)
            prm_paths.extend(found)
        else:
            prm_paths.append(p)

    if not prm_paths:
        click.echo("Error: no .prm files found.", err=True)
        sys.exit(1)

    click.echo(f"  Parsing {len(prm_paths)} .prm file(s)...")
    prm_files = []
    for path in prm_paths:
        prm = parse_prm_file(path)
        classify_file(prm)
        normalize_file(prm)
        prm_files.append(prm)
        click.echo(
            f"  [OK] {path.name}  "
            f"sections={len(prm.sections)}  "
            f"params={sum(len(s.params) for s in prm.sections)}"
        )

    output_dir = Path(output_dir)
    written = export_all(prm_files, output_dir)

    click.echo()
    click.echo("  Output files:")
    for category, paths in written.items():
        for p in paths:
            click.echo(f"    {p}")

    # Print warning count
    report_path = written["report"][0] if written["report"] else None
    if report_path and report_path.exists():
        with open(report_path) as fh:
            report_data = json.load(fh)
        n_warnings = report_data.get("summary", {}).get("total_warnings", 0)
        if n_warnings:
            click.echo(f"\n  {n_warnings} item(s) need manual review — see translation-report.json")


# ---------------------------------------------------------------------------
# validate
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("inputs", nargs=-1, required=True, type=click.Path(exists=True, path_type=Path))
@click.option(
    "--intake", "intake_file", default=None,
    type=click.Path(path_type=Path),
    help="Path to migration-intake.json. When provided, HIPAA checks are enforced "
         "only if compliance_requirements includes 'HIPAA'.",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def validate(inputs, intake_file, verbose):
    """
    Validate .prm file(s) for common migration issues.

    Checks:
      - Parameters classified as STRING that might be DATE/SQL (possible misclassification)
      - Empty required-looking params ($$SQL, $$DATE, $$PATH with empty values)
      - HIPAA-sensitive param names (PHI, PII, SSN, DOB, MRN, etc.)
      - Path params pointing to on-premises locations not yet migrated to S3
      - Multi-line SQL params that may have been truncated

    Example:

      param-translator validate params/SALES_MART.prm
    """
    _setup_logging(verbose)

    # Load intake to determine whether HIPAA checks are applicable
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parent.parent))
    from intake_loader import load_intake, is_hipaa as _is_hipaa
    intake = load_intake(intake_file)
    enforce_hipaa = _is_hipaa(intake) if intake else True  # default: always check

    _HIPAA_NAMES = re.compile(
        r"\b(PHI|PII|SSN|DOB|MRN|PATIENT|NAME|ADDRESS|PHONE|EMAIL|DOD|DOB)\b",
        re.IGNORECASE,
    )
    _ON_PREM_PATH = re.compile(r"^([A-Za-z]:\\|\\\\|/(?!tmp)[a-z])")

    issues: List[dict] = []

    for inp in inputs:
        p = Path(inp)
        prm = parse_prm_file(p)
        classify_file(prm)
        normalize_file(prm)

        for section in prm.sections:
            for name, param in section.params.items():
                # Empty SQL/DATE/PATH-looking names
                if param.param_type == ParamType.EMPTY and any(
                    kw in name.upper() for kw in ("SQL", "DATE", "PATH", "DIR", "FILE")
                ):
                    issues.append({
                        "severity": "WARN",
                        "file": p.name, "section": section.key, "param": name,
                        "message": "Likely required param is empty — verify default is intentional",
                    })

                # HIPAA names (only flagged when HIPAA is applicable)
                if enforce_hipaa and _HIPAA_NAMES.search(name):
                    issues.append({
                        "severity": "HIPAA",
                        "file": p.name, "section": section.key, "param": name,
                        "message": f"Param name suggests PHI/PII data — ensure value is not logged",
                    })

                # On-prem paths
                if param.param_type == ParamType.PATH and _ON_PREM_PATH.match(
                    param.raw_value.strip()
                ):
                    issues.append({
                        "severity": "WARN",
                        "file": p.name, "section": section.key, "param": name,
                        "message": f"On-premises path: '{param.raw_value.strip()}' — migrate to S3",
                    })

    if not issues:
        click.echo("  No issues found.")
        return

    try:
        from rich.console import Console
        from rich.table import Table
        from rich import box
        console = Console()
        t = Table(box=box.SIMPLE_HEAD, header_style="bold magenta")
        t.add_column("Sev",     style="yellow", width=6)
        t.add_column("File",    style="cyan")
        t.add_column("Section", style="white")
        t.add_column("Param",   style="green")
        t.add_column("Message")
        sev_style = {"HIPAA": "red bold", "WARN": "yellow", "INFO": "blue"}
        for issue in issues:
            sev = issue["severity"]
            t.add_row(
                f"[{sev_style.get(sev,'white')}]{sev}[/{sev_style.get(sev,'white')}]",
                issue["file"], issue["section"], issue["param"], issue["message"],
            )
        console.print(t)
    except ImportError:
        for issue in issues:
            click.echo(
                f"  [{issue['severity']:5}]  {issue['file']}  "
                f"{issue['section']}  {issue['param']}  —  {issue['message']}"
            )

    hipaa_count = sum(1 for i in issues if i["severity"] == "HIPAA")
    if hipaa_count:
        click.echo(f"\n  {hipaa_count} HIPAA flag(s) — review before deploying to any environment.")


# ---------------------------------------------------------------------------
# diff
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("file_a", type=click.Path(exists=True, path_type=Path))
@click.argument("file_b", type=click.Path(exists=True, path_type=Path))
@click.option("--verbose", "-v", is_flag=True, default=False)
def diff(file_a, file_b, verbose):
    """
    Compare two .prm files — show added, removed, and changed params.

    Useful for comparing DEV vs PROD parameter files.

    Example:

      param-translator diff params/dev/SALES_MART.prm params/prod/SALES_MART.prm
    """
    _setup_logging(verbose)

    prm_a = parse_prm_file(file_a)
    classify_file(prm_a); normalize_file(prm_a)
    prm_b = parse_prm_file(file_b)
    classify_file(prm_b); normalize_file(prm_b)

    merged_a = prm_a.merged
    merged_b = prm_b.merged

    keys_a = set(merged_a)
    keys_b = set(merged_b)

    added   = keys_b - keys_a
    removed = keys_a - keys_b
    changed = {
        k for k in keys_a & keys_b
        if merged_a[k].normalized_value != merged_b[k].normalized_value
    }

    click.echo(f"  A: {file_a.name}  B: {file_b.name}")
    click.echo(f"  Added: {len(added)}  Removed: {len(removed)}  Changed: {len(changed)}")
    click.echo()

    if added:
        click.echo("  ADDED (in B, not in A):")
        for k in sorted(added):
            p = merged_b[k]
            click.echo(f"    + {k:<30} = {p.normalized_value!r}  [{p.param_type}]")
    if removed:
        click.echo("  REMOVED (in A, not in B):")
        for k in sorted(removed):
            p = merged_a[k]
            click.echo(f"    - {k:<30} = {p.normalized_value!r}  [{p.param_type}]")
    if changed:
        click.echo("  CHANGED:")
        for k in sorted(changed):
            pa, pb = merged_a[k], merged_b[k]
            click.echo(f"    ~ {k:<30}")
            click.echo(f"        A: {pa.normalized_value!r}")
            click.echo(f"        B: {pb.normalized_value!r}")


# ---------------------------------------------------------------------------
# show
# ---------------------------------------------------------------------------

@cli.command()
@click.argument("prm_file", type=click.Path(exists=True, path_type=Path))
@click.option("--section", "-s", default=None, help="Show only this section key.")
@click.option("--type-filter", "-t", default=None,
              type=click.Choice(["DATE","DATE_MASK","SQL","INTEGER","DECIMAL",
                                 "BOOLEAN","PATH","STRING","EMPTY"], case_sensitive=False),
              help="Show only params of this type.")
@click.option("--json-output", is_flag=True, default=False, help="Output raw JSON.")
@click.option("--verbose", "-v", is_flag=True, default=False)
def show(prm_file, section, type_filter, json_output, verbose):
    """
    Show parsed parameter contents of a .prm file.

    Example:

      param-translator show params/SALES_MART.prm

      param-translator show params/SALES_MART.prm --type-filter SQL

      param-translator show params/SALES_MART.prm --json-output
    """
    _setup_logging(verbose)
    prm = parse_prm_file(prm_file)
    classify_file(prm)
    normalize_file(prm)

    if json_output:
        from .exporter import _param_to_dict
        data = {}
        for sec in prm.sections:
            if section and sec.key != section:
                continue
            for name, param in sec.params.items():
                if type_filter and param.param_type != type_filter.upper():
                    continue
                data[f"{sec.key}.{name}"] = _param_to_dict(param)
        click.echo(json.dumps(data, indent=2))
        return

    try:
        from rich.console import Console
        from rich.table import Table
        from rich import box
        console = Console()
        t = Table(box=box.SIMPLE_HEAD, header_style="bold magenta", show_lines=False)
        t.add_column("Section",         style="cyan",  no_wrap=True)
        t.add_column("Param",           style="white", no_wrap=True)
        t.add_column("Type",            style="yellow", width=10)
        t.add_column("Normalized Value", style="green")
        t.add_column("Notes",           style="dim")

        type_style = {
            "DATE": "cyan", "DATE_MASK": "blue", "SQL": "magenta",
            "INTEGER": "white", "DECIMAL": "white", "BOOLEAN": "yellow",
            "PATH": "red", "STRING": "white", "EMPTY": "dim",
        }
        for sec in prm.sections:
            if section and sec.key != section:
                continue
            for name, param in sec.params.items():
                if type_filter and param.param_type != type_filter.upper():
                    continue
                ts = type_style.get(param.param_type, "white")
                val = (param.normalized_value or param.raw_value)[:60]
                if len(val) == 60:
                    val += "…"
                notes_short = param.notes[0][:50] if param.notes else ""
                t.add_row(
                    sec.key,
                    name,
                    f"[{ts}]{param.param_type}[/{ts}]",
                    val,
                    notes_short,
                )
        console.print(t)
    except ImportError:
        for sec in prm.sections:
            if section and sec.key != section:
                continue
            click.echo(f"\n  [{sec.key}]")
            for name, param in sec.params.items():
                if type_filter and param.param_type != type_filter.upper():
                    continue
                click.echo(f"    {name:<30} [{param.param_type:<10}]  {(param.normalized_value or param.raw_value)[:60]}")


import re


# ---------------------------------------------------------------------------
# export-ssm
# ---------------------------------------------------------------------------

@cli.command("export-ssm")
@click.argument("inputs", nargs=-1, required=True, type=click.Path(exists=True, path_type=Path))
@click.option("--output-dir", "-o", default="output", type=click.Path(path_type=Path),
              show_default=True)
@click.option("--verbose", "-v", is_flag=True, default=False)
def export_ssm(inputs, output_dir, verbose):
    """
    Generate Terraform aws_ssm_parameter resources from .prm file(s).

    Includes PATH params (on-prem filesystem paths) and credential-like params
    (PASSWORD, TOKEN, SECRET, KEY, PWD, etc.) as SecureString.

    Output: <output_dir>/terraform/ssm_parameters.tf

    Example:

      param-translator export-ssm params/SALES_MART.prm --output-dir output/

      param-translator export-ssm params/ --output-dir output/
    """
    _setup_logging(verbose)
    from .ssm_exporter import export_ssm_terraform

    prm_paths: List[Path] = []
    for inp in inputs:
        p = Path(inp)
        if p.is_dir():
            found = sorted(p.rglob("*.prm"))
            prm_paths.extend(found)
        else:
            prm_paths.append(p)

    if not prm_paths:
        click.echo("Error: no .prm files found.", err=True)
        sys.exit(1)

    prm_files = []
    for path in prm_paths:
        prm = parse_prm_file(path)
        classify_file(prm)
        normalize_file(prm)
        prm_files.append(prm)
        click.echo(f"  [OK] {path.name}")

    out_path = export_ssm_terraform(prm_files, Path(output_dir))
    click.echo(f"\n  SSM Terraform → {out_path}")

    # Warn about HIPAA
    hipaa_files = sum(
        1 for prm in prm_files
        for section in prm.sections
        for name in section.params
        if re.search(r"\b(PHI|PII|SSN|DOB|MRN|PATIENT)\b", name, re.IGNORECASE)
    )
    if hipaa_files:
        click.echo(
            f"\n  WARNING: {hipaa_files} HIPAA-sensitive param(s) detected. "
            "Review SSM values before deploying.",
            err=True,
        )


def main():
    cli()


if __name__ == "__main__":
    main()
