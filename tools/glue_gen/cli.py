"""
glue-gen CLI

Commands:
  generate-all   Generate Glue scripts + Terraform for all GLUE-routed mappings
  generate       Generate a single mapping by name
  preview        Dry-run — print generated script to stdout without writing files
  report         Print a generation report from a previous run
"""

import json
import logging
import sys
from pathlib import Path
from typing import Optional

import click

from . import __version__
from .generator import generate_all, generate_single


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
    glue-gen — Generate AWS Glue PySpark jobs from a migration manifest.

    Run pc-extractor first to produce migration-manifest.json, then use
    this tool to generate Glue scripts and Terraform for each mapping.
    """


# ---------------------------------------------------------------------------
# generate-all
# ---------------------------------------------------------------------------

@cli.command("generate-all")
@click.argument(
    "manifest_file",
    type=click.Path(exists=True, path_type=Path),
)
@click.option(
    "--output-dir", "-o", default="output",
    type=click.Path(path_type=Path), show_default=True,
    help="Root output directory for generated files.",
)
@click.option(
    "--folder-filter", "-f", "folder_filter", multiple=True,
    help="Only generate for these folder names (can specify multiple times).",
)
@click.option(
    "--include-review", is_flag=True, default=False,
    help="Also generate stub scripts for REVIEW-routed mappings.",
)
@click.option(
    "--params-dir", "params_dir", default=None,
    type=click.Path(path_type=Path),
    help="Directory containing param-translator output (glue-params/ sub-dir). "
         "When set, actual param values are injected into Terraform default_arguments.",
)
@click.option(
    "--env-split", "env_split", is_flag=True, default=False,
    help="Generate multi-environment Terraform (modules/ + environments/dev,staging,prod). "
         "Reads aws_accounts from --intake file when provided.",
)
@click.option(
    "--intake", "intake_file", default=None,
    type=click.Path(path_type=Path),
    help="Path to migration-intake.json for environment-specific values.",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def generate_all_cmd(manifest_file, output_dir, folder_filter, include_review, params_dir, env_split, intake_file, verbose):
    """
    Generate Glue PySpark scripts and Terraform for all GLUE-routed mappings.

    Skips DATABRICKS and REVIEW mappings by default.
    Use --include-review to generate stubs for REVIEW mappings too.

    Example:

      glue-gen generate-all output/migration-manifest.json --output-dir generated/

      glue-gen generate-all output/migration-manifest.json --folder-filter SALES_MART
    """
    _setup_logging(verbose)
    from pc_extractor.xml_parser import _compute_summary
    import json as _json

    click.echo(f"  Loading manifest: {manifest_file}")
    with open(manifest_file, encoding="utf-8") as fh:
        data = _json.load(fh)

    manifest = _load_manifest(data)

    click.echo(
        f"  Manifest: {manifest.repository_name}  "
        f"({manifest.summary.total_mappings} mappings)"
    )
    click.echo(f"  GLUE-routed: {manifest.summary.routed_glue}")
    click.echo(f"  Generating into: {output_dir}")
    click.echo()

    intake: Optional[dict] = None
    if intake_file:
        import json as _ijson
        with open(intake_file, encoding="utf-8") as fh:
            intake = _ijson.load(fh)
        click.echo(f"  Intake: {intake_file}")

    report = generate_all(
        manifest,
        output_dir=output_dir,
        folder_filter=list(folder_filter) if folder_filter else None,
        include_review=include_review,
        params_dir=params_dir,
        env_split=env_split,
        intake=intake,
    )

    _print_report(report)

    if report.errors:
        sys.exit(1)


# ---------------------------------------------------------------------------
# generate (single)
# ---------------------------------------------------------------------------

@cli.command("generate")
@click.argument("manifest_file", type=click.Path(exists=True, path_type=Path))
@click.argument("folder_name")
@click.argument("mapping_name")
@click.option(
    "--output-dir", "-o", default="output",
    type=click.Path(path_type=Path), show_default=True,
)
@click.option(
    "--params-dir", "params_dir", default=None,
    type=click.Path(path_type=Path),
    help="Directory containing param-translator output.",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def generate_cmd(manifest_file, folder_name, mapping_name, output_dir, params_dir, verbose):
    """
    Generate a single mapping by folder and mapping name.

    Example:

      glue-gen generate output/migration-manifest.json SALES_MART M_LOAD_FACT_ORDERS
    """
    _setup_logging(verbose)
    import json as _json

    with open(manifest_file, encoding="utf-8") as fh:
        data = _json.load(fh)
    manifest = _load_manifest(data)

    result = generate_single(manifest, folder_name, mapping_name, output_dir, params_dir=params_dir)

    if result.status == "SUCCESS":
        click.echo(f"  [OK]  {result.glue_script_path}")
        click.echo(f"  [OK]  {result.terraform_path}")
        if result.warnings:
            click.echo(f"\n  Warnings ({len(result.warnings)}):")
            for w in result.warnings:
                click.echo(f"    - {w}")
    elif result.status == "SKIPPED":
        click.echo(f"  [SKIP] {folder_name}/{mapping_name} — platform: {result.target_platform}")
    else:
        click.echo(f"  [ERROR] {result.error}", err=True)
        sys.exit(1)


# ---------------------------------------------------------------------------
# preview
# ---------------------------------------------------------------------------

@cli.command("preview")
@click.argument("manifest_file", type=click.Path(exists=True, path_type=Path))
@click.argument("folder_name")
@click.argument("mapping_name")
@click.option(
    "--terraform", is_flag=True, default=False,
    help="Show Terraform HCL instead of the Glue script.",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def preview_cmd(manifest_file, folder_name, mapping_name, terraform, verbose):
    """
    Preview generated code for a mapping without writing any files.

    Example:

      glue-gen preview output/migration-manifest.json SALES_MART M_LOAD_FACT_ORDERS

      glue-gen preview output/migration-manifest.json SALES_MART M_LOAD_FACT_ORDERS --terraform
    """
    _setup_logging(verbose)
    import json as _json
    from .code_builder import build_glue_script
    from .tf_builder import build_terraform_job
    from .generator import _extract_args_from_script

    with open(manifest_file, encoding="utf-8") as fh:
        data = _json.load(fh)
    manifest = _load_manifest(data)

    folder = manifest.folders.get(folder_name)
    if not folder:
        click.echo(f"Error: folder '{folder_name}' not found", err=True)
        sys.exit(1)
    mapping = folder.mappings.get(mapping_name)
    if not mapping:
        click.echo(f"Error: mapping '{mapping_name}' not found in '{folder_name}'", err=True)
        sys.exit(1)

    if terraform:
        script_text, _ = build_glue_script(mapping, folder)
        args = _extract_args_from_script(script_text)
        click.echo(build_terraform_job(mapping, args))
    else:
        script_text, warnings = build_glue_script(mapping, folder)
        click.echo(script_text)
        if warnings:
            click.echo(f"\n# --- {len(warnings)} WARNING(S) ---", err=True)
            for w in warnings:
                click.echo(f"# {w}", err=True)


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------

@cli.command("report")
@click.argument(
    "report_file",
    type=click.Path(exists=True, path_type=Path),
)
def report_cmd(report_file):
    """
    Print a generation report from a previous generate-all run.

    Example:

      glue-gen report output/generation-report.json
    """
    with open(report_file, encoding="utf-8") as fh:
        data = json.load(fh)

    report_obj_data = type("R", (), data)()
    results = [type("M", (), r)() for r in data.get("results", [])]

    try:
        from rich.console import Console
        from rich.table import Table
        from rich import box

        console = Console()
        console.rule("[bold cyan]GENERATION REPORT[/bold cyan]")
        console.print(f"  Generated at: {data.get('generated_at', '')}")
        console.print(
            f"  Total: [bold]{data.get('total_mappings', 0)}[/bold]  "
            f"Generated: [green]{data.get('generated', 0)}[/green]  "
            f"Skipped: [yellow]{data.get('skipped', 0)}[/yellow]  "
            f"Errors: [red]{data.get('errors', 0)}[/red]"
        )
        console.print()

        t = Table(box=box.SIMPLE_HEAD, show_header=True, header_style="bold magenta")
        t.add_column("Folder",   style="cyan")
        t.add_column("Mapping")
        t.add_column("Status",   justify="center")
        t.add_column("Platform", justify="center")
        t.add_column("Score",    justify="right")
        t.add_column("Warnings", justify="right")

        status_style = {"SUCCESS": "green", "SKIPPED": "yellow", "ERROR": "red bold"}
        for r in data.get("results", []):
            s = r.get("status", "")
            style = status_style.get(s, "white")
            t.add_row(
                r.get("folder", ""),
                r.get("mapping", ""),
                f"[{style}]{s}[/{style}]",
                r.get("target_platform", ""),
                str(r.get("complexity_score", "")),
                str(len(r.get("warnings", []))),
            )
        console.print(t)
    except ImportError:
        print(f"Generated: {data.get('generated')}  Skipped: {data.get('skipped')}  Errors: {data.get('errors')}")
        for r in data.get("results", []):
            print(f"  {r.get('status'):8}  {r.get('folder')}/{r.get('mapping')}")


# ---------------------------------------------------------------------------
# Manifest loader (reconstruct from JSON without full pc_extractor parse)
# ---------------------------------------------------------------------------

def _load_manifest(data: dict):
    """
    Reconstruct a MigrationManifest from saved JSON.
    Uses pc_extractor models directly.
    """
    import sys
    sys.path.insert(0, str(Path(__file__).parent.parent))

    from pc_extractor.models import (
        MigrationManifest, FolderDef, SourceDef, TargetDef, FieldDef,
        MappingDef, MappingFlags, MappingVariableDef, TransformationDef,
        TransformationType, PortDef, ConnectorDef, InstanceDef,
        RouterGroupDef, WorkflowDef, WorkflowTaskDef, WorkflowLinkDef,
        SchedulerDef, ExtractionSummary, TargetPlatform,
    )

    def _load_field(d) -> FieldDef:
        return FieldDef(**{k: v for k, v in d.items() if k in FieldDef.__dataclass_fields__})

    def _load_source(d) -> SourceDef:
        s = SourceDef(
            name=d["name"], db_type=d["db_type"],
            db_name=d.get("db_name", ""), owner=d.get("owner", ""),
            description=d.get("description", ""),
            is_fixed_width=d.get("is_fixed_width", False),
            delimiter=d.get("delimiter", ""),
            codepage=d.get("codepage", ""),
        )
        s.fields = [_load_field(f) for f in d.get("fields", [])]
        return s

    def _load_target(d) -> TargetDef:
        t = TargetDef(
            name=d["name"], db_type=d["db_type"],
            db_name=d.get("db_name", ""), owner=d.get("owner", ""),
            description=d.get("description", ""),
        )
        t.fields = [_load_field(f) for f in d.get("fields", [])]
        return t

    def _load_port(d) -> PortDef:
        return PortDef(**{k: v for k, v in d.items() if k in PortDef.__dataclass_fields__})

    def _load_router_group(d) -> RouterGroupDef:
        return RouterGroupDef(name=d["name"], condition=d.get("condition", ""))

    def _load_transformation(d) -> TransformationDef:
        t = TransformationDef(
            name=d["name"],
            type=TransformationType.from_str(d.get("type", "")),
            reusable=d.get("reusable", False),
            description=d.get("description", ""),
            sql_query=d.get("sql_query"),
            filter_condition=d.get("filter_condition"),
            lookup_condition=d.get("lookup_condition"),
            stored_proc_name=d.get("stored_proc_name"),
            join_condition=d.get("join_condition"),
            join_type=d.get("join_type"),
        )
        t.ports = [_load_port(p) for p in d.get("ports", [])]
        t.attributes = d.get("attributes", {})
        t.router_groups = [_load_router_group(g) for g in d.get("router_groups", [])]
        return t

    def _load_mapping(d) -> MappingDef:
        flags_d = d.get("flags", {})
        flags = MappingFlags(
            has_stored_proc=flags_d.get("has_stored_proc", False),
            has_parameter_vars=flags_d.get("has_parameter_vars", False),
            has_sql_override=flags_d.get("has_sql_override", False),
            has_custom_transform=flags_d.get("has_custom_transform", False),
            has_xml=flags_d.get("has_xml", False),
            has_normalizer=flags_d.get("has_normalizer", False),
            has_joiner=flags_d.get("has_joiner", False),
            has_lookup=flags_d.get("has_lookup", False),
            has_router=flags_d.get("has_router", False),
            has_update_strategy=flags_d.get("has_update_strategy", False),
            has_sequence_gen=flags_d.get("has_sequence_gen", False),
            multi_source=flags_d.get("multi_source", False),
            source_db_types=flags_d.get("source_db_types", []),
            target_db_types=flags_d.get("target_db_types", []),
            transformation_type_counts=flags_d.get("transformation_type_counts", {}),
        )
        m = MappingDef(
            name=d["name"], folder=d["folder"],
            description=d.get("description", ""),
            is_valid=d.get("is_valid", True),
            sources=d.get("sources", []),
            targets=d.get("targets", []),
            flags=flags,
            complexity_score=d.get("complexity_score"),
            complexity_reasons=d.get("complexity_reasons", []),
            review_notes=d.get("review_notes", []),
        )
        tp = d.get("target_platform")
        m.target_platform = TargetPlatform(tp) if tp else None
        m.transformations = [_load_transformation(t) for t in d.get("transformations", [])]
        m.connectors = [
            ConnectorDef(**c) for c in d.get("connectors", [])
        ]
        m.variables = [
            MappingVariableDef(**{k: v for k, v in v.items() if k in MappingVariableDef.__dataclass_fields__})
            for v in d.get("variables", [])
        ]
        m.instances = [
            InstanceDef(**{k: v for k, v in i.items() if k in InstanceDef.__dataclass_fields__})
            for i in d.get("instances", [])
        ]
        return m

    def _load_folder(d) -> FolderDef:
        f = FolderDef(name=d["name"], description=d.get("description", ""))
        f.sources  = {k: _load_source(v)  for k, v in d.get("sources", {}).items()}
        f.targets  = {k: _load_target(v)  for k, v in d.get("targets", {}).items()}
        f.mappings = {k: _load_mapping(v) for k, v in d.get("mappings", {}).items()}
        return f

    summary_d = data.get("summary", {})
    from pc_extractor.models import ExtractionSummary
    s_fields = ExtractionSummary.__dataclass_fields__
    summary = ExtractionSummary(**{k: summary_d.get(k, v.default if v.default is not v.default_factory else [])  # type: ignore
                                    for k, v in s_fields.items()})

    manifest = MigrationManifest(
        extracted_at=data.get("extracted_at", ""),
        source_files=data.get("source_files", []),
        repository_name=data.get("repository_name", ""),
        summary=summary,
    )
    manifest.folders = {k: _load_folder(v) for k, v in data.get("folders", {}).items()}
    return manifest


def main():
    cli()


if __name__ == "__main__":
    main()
