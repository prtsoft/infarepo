"""
workflow-gen CLI

Commands:
  generate-all   Generate orchestration for all workflows in a manifest
  generate       Generate a single workflow by folder+name
"""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path
from typing import List, Optional

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
    workflow-gen — Generate orchestration artifacts from a migration manifest.

    Converts PowerCenter WORKFLOW definitions to:
      step-functions  AWS Step Functions State Machine JSON + Terraform
      glue-workflow   AWS Glue Workflow + Trigger Terraform HCL
      airflow         Apache Airflow 2.x DAG Python files
      stub            Placeholder files for manual implementation

    Run pc-extractor first to produce migration-manifest.json.
    """


# ---------------------------------------------------------------------------
# generate-all
# ---------------------------------------------------------------------------

@cli.command("generate-all")
@click.argument("manifest_json", type=click.Path(exists=True, path_type=Path))
@click.option(
    "--target", "-t",
    type=click.Choice(["step-functions", "glue-workflow", "airflow", "stub"]),
    default="stub", show_default=True,
    help="Orchestration target to generate.",
)
@click.option(
    "--output-dir", "-o", default="output",
    type=click.Path(path_type=Path), show_default=True,
)
@click.option(
    "--folder", "-f", "folder_filter", multiple=True,
    help="Only generate for these folder names (can specify multiple times).",
)
@click.option(
    "--generation-report", "report_path", default=None,
    type=click.Path(path_type=Path),
    help="Path to generation-report.json from glue-gen/databricks-gen. "
         "Used to wire session task names to actual job names.",
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def generate_all_cmd(manifest_json, target, output_dir, folder_filter, report_path, verbose):
    """
    Generate workflow orchestration for all workflows in the manifest.

    Examples:

      workflow-gen generate-all manifest.json --target step-functions --output-dir output/

      workflow-gen generate-all manifest.json --target glue-workflow --folder SALES_MART

      workflow-gen generate-all manifest.json --target airflow \\
          --generation-report output/generation-report.json
    """
    _setup_logging(verbose)

    click.echo(f"  Loading manifest: {manifest_json}")
    with open(manifest_json, encoding="utf-8") as fh:
        data = json.load(fh)

    manifest = _load_manifest(data)
    click.echo(f"  Target: {target}  |  Generating into: {output_dir}")
    click.echo()

    report = generate_all(
        manifest,
        output_dir=output_dir,
        target=target,
        folder_filter=list(folder_filter) if folder_filter else None,
        generation_report_path=report_path,
    )

    click.echo(
        f"  Generated: {report.generated}  "
        f"Skipped: {report.skipped}  "
        f"Errors: {report.errors}  "
        f"Total: {report.total}"
    )
    for r in report.results:
        if not r.skipped and not r.error:
            for p in r.output_paths:
                click.echo(f"  [OK]  {p}")
            for w in r.warnings:
                click.echo(f"        ! {w}")
        elif r.skipped:
            click.echo(f"  [SKIP] {r.folder}/{r.workflow} — {r.skip_reason}")
        else:
            click.echo(f"  [ERROR] {r.folder}/{r.workflow} — {r.error}", err=True)

    if report.errors:
        sys.exit(1)


# ---------------------------------------------------------------------------
# generate (single)
# ---------------------------------------------------------------------------

@cli.command("generate")
@click.argument("manifest_json", type=click.Path(exists=True, path_type=Path))
@click.argument("folder")
@click.argument("workflow")
@click.option(
    "--target", "-t",
    type=click.Choice(["step-functions", "glue-workflow", "airflow", "stub"]),
    default="stub", show_default=True,
)
@click.option(
    "--output-dir", "-o", default="output",
    type=click.Path(path_type=Path), show_default=True,
)
@click.option(
    "--generation-report", "report_path", default=None,
    type=click.Path(path_type=Path),
)
@click.option("--verbose", "-v", is_flag=True, default=False)
def generate_cmd(manifest_json, folder, workflow, target, output_dir, report_path, verbose):
    """
    Generate orchestration for a single workflow by folder and name.

    Example:

      workflow-gen generate manifest.json SALES_MART WF_DAILY --target step-functions
    """
    _setup_logging(verbose)

    with open(manifest_json, encoding="utf-8") as fh:
        data = json.load(fh)
    manifest = _load_manifest(data)

    result = generate_single(
        manifest, folder, workflow,
        output_dir=output_dir,
        target=target,
        generation_report_path=report_path,
    )

    if result.skipped:
        click.echo(f"  [SKIP] {folder}/{workflow} — {result.skip_reason}")
    elif result.error:
        click.echo(f"  [ERROR] {result.error}", err=True)
        sys.exit(1)
    else:
        for p in result.output_paths:
            click.echo(f"  [OK]  {p}")
        for w in result.warnings:
            click.echo(f"  ! {w}")


# ---------------------------------------------------------------------------
# Manifest loader (same pattern as glue_gen)
# ---------------------------------------------------------------------------

def _load_manifest(data: dict):
    import sys as _sys
    _sys.path.insert(0, str(Path(__file__).parent.parent))

    from pc_extractor.models import (
        MigrationManifest, FolderDef, MappingDef, WorkflowDef,
        WorkflowTaskDef, WorkflowLinkDef, SchedulerDef,
        MappingFlags, TargetPlatform, ExtractionSummary,
        SourceDef, TargetDef, FieldDef, TransformationDef,
        TransformationType, PortDef, ConnectorDef, InstanceDef,
        RouterGroupDef, MappingVariableDef,
    )

    def _load_scheduler(d: dict) -> SchedulerDef:
        return SchedulerDef(
            schedule_type=d.get("schedule_type", ""),
            start_time=d.get("start_time", ""),
            end_time=d.get("end_time", ""),
            raw_attributes=d.get("raw_attributes", {}),
        )

    def _load_task(d: dict) -> WorkflowTaskDef:
        return WorkflowTaskDef(
            name=d["name"],
            task_type=d.get("task_type", "SESSION"),
            is_enabled=d.get("is_enabled", True),
            is_reusable=d.get("is_reusable", False),
            mapping_ref=d.get("mapping_ref"),
            description=d.get("description", ""),
        )

    def _load_link(d: dict) -> WorkflowLinkDef:
        return WorkflowLinkDef(
            from_task=d["from_task"],
            to_task=d["to_task"],
            condition=d.get("condition", ""),
        )

    def _load_workflow(d: dict) -> WorkflowDef:
        wf = WorkflowDef(
            name=d["name"],
            folder=d.get("folder", ""),
            description=d.get("description", ""),
            is_enabled=d.get("is_enabled", True),
            is_valid=d.get("is_valid", True),
            server_name=d.get("server_name", ""),
            scheduler=_load_scheduler(d.get("scheduler", {})),
        )
        wf.tasks = [_load_task(t) for t in d.get("tasks", [])]
        wf.links = [_load_link(lnk) for lnk in d.get("links", [])]
        return wf

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
        return m

    def _load_folder(d) -> FolderDef:
        f = FolderDef(name=d["name"], description=d.get("description", ""))
        f.sources  = {k: _load_source(v)  for k, v in d.get("sources", {}).items()}
        f.targets  = {k: _load_target(v)  for k, v in d.get("targets", {}).items()}
        f.mappings = {k: _load_mapping(v) for k, v in d.get("mappings", {}).items()}
        f.workflows = {k: _load_workflow(v) for k, v in d.get("workflows", {}).items()}
        return f

    summary_d = data.get("summary", {})
    s_fields = ExtractionSummary.__dataclass_fields__
    summary = ExtractionSummary(**{
        k: summary_d.get(k, v.default if v.default is not v.default_factory else [])  # type: ignore
        for k, v in s_fields.items()
    })

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
