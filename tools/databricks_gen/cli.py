"""
databricks-gen CLI

Commands:
  generate-all   Generate notebooks + Terraform for all DATABRICKS-routed mappings
  generate       Generate a single mapping by name
  report         Pretty-print a generation report from a previous run
"""

import json
import logging
import sys
from pathlib import Path
from typing import Optional

import click

from . import __version__
from .generator import generate_all, generate_single, _load_manifest


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
    databricks-gen — Generate Databricks notebooks + Terraform from a migration manifest.

    Run pc-extractor first to produce the manifest JSON, then use this tool
    to generate Databricks notebooks and Terraform HCL for DATABRICKS-routed mappings.
    """


# ---------------------------------------------------------------------------
# generate-all
# ---------------------------------------------------------------------------

@cli.command("generate-all")
@click.argument(
    "manifest_json",
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "--output-dir", "-o", default="output",
    type=click.Path(path_type=Path), show_default=True,
    help="Root output directory for generated files.",
)
@click.option(
    "--folder", "-f", "folder_filter", multiple=True,
    help="Only generate for these folder names (can be specified multiple times).",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def generate_all_cmd(manifest_json, output_dir, folder_filter, verbose):
    """
    Generate Databricks notebooks and Terraform for all DATABRICKS-routed mappings.

    Skips GLUE and REVIEW mappings.

    Example:

      databricks-gen generate-all output/manifest.json --output-dir generated/

      databricks-gen generate-all output/manifest.json --folder PATIENT_EXTRACT
    """
    _setup_logging(verbose)

    click.echo(f"  Loading manifest: {manifest_json}")
    with open(manifest_json, encoding="utf-8") as fh:
        data = json.load(fh)

    manifest = _load_manifest(data)

    click.echo(
        f"  Manifest: {manifest.repository_name}  "
        f"({manifest.summary.total_mappings} mappings)"
    )
    click.echo(f"  DATABRICKS-routed: {manifest.summary.routed_databricks}")
    click.echo(f"  Generating into: {output_dir}")
    click.echo()

    report = generate_all(
        manifest,
        output_dir=output_dir,
        folder_filter=list(folder_filter) if folder_filter else None,
    )

    _print_report(report)


# ---------------------------------------------------------------------------
# generate (single)
# ---------------------------------------------------------------------------

@cli.command("generate")
@click.argument("manifest_json", type=click.Path(exists=True, path_type=Path))
@click.argument("folder")
@click.argument("mapping")
@click.option(
    "--output-dir", "-o", default="output",
    type=click.Path(path_type=Path), show_default=True,
)
@click.option(
    "--preview", is_flag=True, default=False,
    help="Print the generated notebook to stdout without writing files.",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def generate_cmd(manifest_json, folder, mapping, output_dir, preview, verbose):
    """
    Generate a single mapping by folder and mapping name.

    Example:

      databricks-gen generate output/manifest.json PATIENT_EXTRACT M_EXTRACT_PATIENT_DELTA
    """
    _setup_logging(verbose)

    with open(manifest_json, encoding="utf-8") as fh:
        data = json.load(fh)
    manifest = _load_manifest(data)

    if preview:
        from .notebook_builder import DatabricksNotebookBuilder, render_notebook
        folder_obj = manifest.folders.get(folder)
        if not folder_obj:
            click.echo(f"Error: folder '{folder}' not found", err=True)
            sys.exit(1)
        mapping_obj = folder_obj.mappings.get(mapping)
        if not mapping_obj:
            click.echo(f"Error: mapping '{mapping}' not found in '{folder}'", err=True)
            sys.exit(1)
        builder = DatabricksNotebookBuilder()
        notebook = builder.build(mapping_obj)
        click.echo(render_notebook(notebook))
        if notebook.warnings:
            click.echo(f"\n# --- {len(notebook.warnings)} WARNING(S) ---", err=True)
            for w in notebook.warnings:
                click.echo(f"# {w}", err=True)
        return

    result = generate_single(manifest, folder, mapping, output_dir)

    if result.skipped:
        click.echo(f"  [SKIP] {folder}/{mapping} — platform: {result.skip_reason}")
    else:
        if result.notebook_path:
            click.echo(f"  [OK]  {result.notebook_path}")
        if result.tf_path:
            click.echo(f"  [OK]  {result.tf_path}")
        if result.warnings:
            click.echo(f"\n  Warnings ({len(result.warnings)}):")
            for w in result.warnings:
                click.echo(f"    - {w}")


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

@cli.command("report")
@click.argument(
    "generation_report_json",
    type=click.Path(exists=True, path_type=Path),
)
def report_cmd(generation_report_json):
    """
    Pretty-print a generation report from a previous generate-all run.

    Example:

      databricks-gen report output/databricks-generation-report.json
    """
    with open(generation_report_json, encoding="utf-8") as fh:
        data = json.load(fh)

    try:
        from rich.console import Console
        from rich.table import Table
        from rich import box

        console = Console()
        console.rule("[bold cyan]DATABRICKS GENERATION REPORT[/bold cyan]")
        console.print(
            f"  Total: [bold]{data.get('total', 0)}[/bold]  "
            f"Generated: [green]{data.get('generated', 0)}[/green]  "
            f"Skipped: [yellow]{data.get('skipped', 0)}[/yellow]  "
            f"Warnings: [magenta]{data.get('warnings_count', 0)}[/magenta]"
        )
        console.print()

        t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold magenta")
        t.add_column("Folder", style="cyan")
        t.add_column("Mapping")
        t.add_column("Skipped", justify="center")
        t.add_column("Warnings", justify="right")
        t.add_column("Notebook Path")

        for r in data.get("results", []):
            skipped = r.get("skipped", False)
            skip_style = "yellow" if skipped else "green"
            skip_label = f"[{skip_style}]{'YES' if skipped else 'NO'}[/{skip_style}]"
            t.add_row(
                r.get("folder", ""),
                r.get("mapping_name", ""),
                skip_label,
                str(len(r.get("warnings", []))),
                r.get("notebook_path") or r.get("skip_reason", ""),
            )
        console.print(t)
    except ImportError:
        print(
            f"Total: {data.get('total')}  "
            f"Generated: {data.get('generated')}  "
            f"Skipped: {data.get('skipped')}  "
            f"Warnings: {data.get('warnings_count')}"
        )
        for r in data.get("results", []):
            status = "SKIP" if r.get("skipped") else "OK"
            print(f"  [{status:4}]  {r.get('folder')}/{r.get('mapping_name')}")


def _print_report(report) -> None:
    """Print a brief report summary to stdout."""
    click.echo(
        f"  Generated: {report.generated}  "
        f"Skipped: {report.skipped}  "
        f"Warnings: {report.warnings_count}  "
        f"Total: {report.total}"
    )
    for r in report.results:
        if not r.skipped:
            status = "OK"
            detail = r.notebook_path or ""
        else:
            status = "SKIP"
            detail = r.skip_reason
        click.echo(f"  [{status:4}]  {r.folder}/{r.mapping_name}  {detail}")
        if r.warnings:
            for w in r.warnings:
                click.echo(f"          ! {w}")


def main():
    cli()


if __name__ == "__main__":
    main()
