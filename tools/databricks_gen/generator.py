"""
Top-level orchestrator for databricks-gen.

For each DATABRICKS-routed mapping:
  1. Build Databricks Python notebook (notebook_builder)
  2. Build Terraform HCL (tf_builder)
  3. Write files to output directory

Output layout:
  <output_dir>/
    notebooks/
      <FOLDER>/
        <MAPPING_NAME>.py
    terraform/
      <FOLDER>/
        <MAPPING_NAME>.tf
        variables.tf           (one per folder)
    databricks-generation-report.json
"""

from __future__ import annotations

import json
import logging
import sys
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pc_extractor.models import (
    MigrationManifest,
    FolderDef,
    MappingDef,
    TargetPlatform,
    FieldDef,
    SourceDef,
    TargetDef,
    TransformationDef,
    TransformationType,
    PortDef,
    ConnectorDef,
    InstanceDef,
    RouterGroupDef,
    MappingFlags,
    MappingVariableDef,
    WorkflowDef,
    WorkflowTaskDef,
    WorkflowLinkDef,
    SchedulerDef,
    ExtractionSummary,
)
from .models import GenerationReport, NotebookGenerationResult
from .notebook_builder import DatabricksNotebookBuilder, render_notebook
from .tf_builder import (
    build_terraform_job,
    build_terraform_variables,
    build_databricks_job_module,
    build_databricks_environment_main,
    build_databricks_environment_variables,
    build_databricks_environment_tfvars,
    build_databricks_backend_tf,
)


# ---------------------------------------------------------------------------
# Session param loader (same strategy as glue_gen.generator)
# ---------------------------------------------------------------------------

def _load_session_params(params_dir: Path, folder_name: str, mapping_name: str) -> dict:
    """Load param-translator output for a specific folder+mapping."""
    if not params_dir:
        return {}

    glue_params_dir = Path(params_dir) / "glue-params" / folder_name
    if not glue_params_dir.is_dir():
        return {}

    mapping_upper = mapping_name.upper()
    for json_file in glue_params_dir.glob("*.json"):
        stem_parts = json_file.stem.upper().split(".")
        if len(stem_parts) >= 2 and stem_parts[-1] == mapping_upper:
            try:
                data = json.loads(json_file.read_text(encoding="utf-8"))
                return {k: v for k, v in data.items() if not k.startswith("_")}
            except Exception:
                pass

    for json_file in glue_params_dir.glob("*.json"):
        try:
            data = json.loads(json_file.read_text(encoding="utf-8"))
            section = data.get("_metadata", {}).get("section", "")
            task_part = section.split(":")[-1].upper() if ":" in section else ""
            if task_part == mapping_upper:
                return {k: v for k, v in data.items() if not k.startswith("_")}
        except Exception:
            pass

    merged_dir = Path(params_dir) / "glue-params" / "_merged"
    if merged_dir.is_dir():
        merged_files = list(merged_dir.glob("*.json"))
        if merged_files:
            try:
                data = json.loads(merged_files[0].read_text(encoding="utf-8"))
                return {k: v for k, v in data.items() if not k.startswith("_")}
            except Exception:
                pass

    return {}

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Per-mapping generator
# ---------------------------------------------------------------------------

def _generate_mapping(
    mapping: MappingDef,
    output_dir: Path,
    folder_sources: Optional[dict] = None,
    folder_targets: Optional[dict] = None,
    session_params: Optional[dict] = None,
    sp_strategy: str = "databricks-call",
) -> NotebookGenerationResult:
    result = NotebookGenerationResult(
        mapping_name=mapping.name,
        folder=mapping.folder,
    )

    # Build notebook
    try:
        builder = DatabricksNotebookBuilder()
        notebook = builder.build(
            mapping,
            folder_sources=folder_sources,
            folder_targets=folder_targets,
            session_params=session_params,
            sp_strategy=sp_strategy,
        )
        result.warnings = list(notebook.warnings)

        nb_dir = output_dir / "notebooks" / mapping.folder
        nb_dir.mkdir(parents=True, exist_ok=True)
        nb_path = nb_dir / f"{mapping.name}.py"
        nb_path.write_text(render_notebook(notebook), encoding="utf-8")
        result.notebook_path = str(nb_path)
        log.info("  [NB]  %-50s  warnings=%d", mapping.name, len(notebook.warnings))

        # Inline ruff lint (syntax errors only — E9, F rules)
        try:
            import subprocess as _sp
            ruff = _sp.run(
                ["ruff", "check", "--select=E9,F", "--output-format=text", str(nb_path)],
                capture_output=True, text=True,
            )
            if ruff.returncode != 0:
                for line in ruff.stdout.splitlines():
                    if line.strip():
                        result.warnings.append(f"ruff: {line}")
                        log.warning("  [RUFF] %s", line)
        except FileNotFoundError:
            pass  # ruff not installed — skip silently
    except Exception as exc:
        log.error("  [ERROR] %s / %s: %s", mapping.folder, mapping.name, exc)
        result.warnings.append(f"Notebook generation failed: {exc}")

    # Build Terraform
    try:
        notebook_ws_path = f"/Repos/migration/{mapping.folder}/{mapping.name}"
        tf_text = build_terraform_job(mapping, notebook_ws_path)

        tf_dir = output_dir / "terraform" / mapping.folder
        tf_dir.mkdir(parents=True, exist_ok=True)
        tf_path = tf_dir / f"{mapping.name}.tf"
        tf_path.write_text(tf_text, encoding="utf-8")
        result.tf_path = str(tf_path)
        log.info("  [TF]  %-50s", mapping.name)
    except Exception as exc:
        log.warning("  [WARN] Terraform generation failed for %s: %s", mapping.name, exc)
        result.warnings.append(f"Terraform generation failed: {exc}")

    return result


# ---------------------------------------------------------------------------
# Variables.tf — one per folder (idempotent)
# ---------------------------------------------------------------------------

def _generate_folder_variables(folder_name: str, output_dir: Path) -> None:
    vars_text = build_terraform_variables(folder_name)
    tf_dir = output_dir / "terraform" / folder_name
    tf_dir.mkdir(parents=True, exist_ok=True)
    (tf_dir / "variables.tf").write_text(vars_text, encoding="utf-8")


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def _generate_db_env_split(
    folder_name: str,
    mapping_names: List[str],
    output_dir: Path,
    intake: Optional[dict] = None,
) -> None:
    """Write modules/ + environments/ layout for Databricks multi-env Terraform."""
    intake = intake or {}
    aws_accounts: dict = intake.get("aws_accounts", {})
    aws_region: str = intake.get("aws_region", "us-east-1")
    tf_state: dict = intake.get("terraform_state", {})
    s3_bucket    = tf_state.get("s3_bucket", "")
    dynamo_table = tf_state.get("dynamodb_table", "")
    db_config: dict = intake.get("databricks", {})

    mod_dir = output_dir / "terraform" / "modules" / "databricks_job"
    mod_dir.mkdir(parents=True, exist_ok=True)
    (mod_dir / "main.tf").write_text(build_databricks_job_module(), encoding="utf-8")

    envs = list(aws_accounts.keys()) if aws_accounts else ["dev", "staging", "prod"]

    for env in envs:
        env_data = aws_accounts.get(env, {})
        region      = env_data.get("region", aws_region)
        account_id  = env_data.get("account_id", "")
        db_env      = db_config.get(env, {})
        workspace   = db_env.get("workspace_url", "")
        cluster_id  = db_env.get("cluster_id", "")

        env_dir = output_dir / "terraform" / "environments" / env
        env_dir.mkdir(parents=True, exist_ok=True)

        (env_dir / "main.tf").write_text(
            build_databricks_environment_main(folder_name, mapping_names, env),
            encoding="utf-8",
        )
        (env_dir / "variables.tf").write_text(
            build_databricks_environment_variables(),
            encoding="utf-8",
        )
        (env_dir / "terraform.tfvars").write_text(
            build_databricks_environment_tfvars(env, workspace, cluster_id),
            encoding="utf-8",
        )
        (env_dir / "backend.tf").write_text(
            build_databricks_backend_tf(env, s3_bucket, dynamo_table, region, folder_name),
            encoding="utf-8",
        )
    log.info("  [ENV-SPLIT] %s → %d environments", folder_name, len(envs))


def generate_all(
    manifest: MigrationManifest,
    output_dir: Path,
    folder_filter: Optional[List[str]] = None,
    params_dir: Optional[Path] = None,
    env_split: bool = False,
    intake: Optional[dict] = None,
    sp_strategy: str = "databricks-call",
) -> GenerationReport:
    """
    Generate Databricks notebooks + Terraform for all DATABRICKS-routed mappings.

    Args:
        manifest:       Parsed migration manifest.
        output_dir:     Root output directory.
        folder_filter:  If set, only process these folder names.

    Returns:
        GenerationReport summarising what was generated.
    """
    output_dir = Path(output_dir)
    report = GenerationReport()

    folders_with_results: Dict[str, List[NotebookGenerationResult]] = {}

    for folder_name, folder in manifest.folders.items():
        if folder_filter and folder_name not in folder_filter:
            continue

        folder_results: List[NotebookGenerationResult] = []

        for mapping in folder.mappings.values():
            report.total += 1
            platform = mapping.target_platform

            if platform != TargetPlatform.DATABRICKS:
                log.debug("Skipping %s/%s (platform=%s)", folder_name, mapping.name, platform)
                result = NotebookGenerationResult(
                    mapping_name=mapping.name,
                    folder=folder_name,
                    skipped=True,
                    skip_reason=platform.value if platform else "UNKNOWN",
                )
                report.skipped += 1
                report.results.append(result)
                continue

            sp = _load_session_params(params_dir, folder_name, mapping.name) if params_dir else {}
            result = _generate_mapping(
                mapping, output_dir,
                folder_sources=folder.sources,
                folder_targets=folder.targets,
                session_params=sp,
                sp_strategy=sp_strategy,
            )
            folder_results.append(result)
            report.results.append(result)
            report.generated += 1
            report.warnings_count += len(result.warnings)

        if folder_results:
            _generate_folder_variables(folder_name, output_dir)
            folders_with_results[folder_name] = folder_results
            if env_split:
                names = [r.mapping_name for r in folder_results if not r.skipped and r.notebook_path]
                _generate_db_env_split(folder_name, names, output_dir, intake=intake)

    _write_report(report, output_dir)
    log.info(
        "Summary: generated=%d  skipped=%d  warnings=%d  total=%d",
        report.generated, report.skipped, report.warnings_count, report.total,
    )
    return report


def generate_single(
    manifest: MigrationManifest,
    folder_name: str,
    mapping_name: str,
    output_dir: Path,
    params_dir: Optional[Path] = None,
) -> NotebookGenerationResult:
    """Generate a single mapping by folder and mapping name."""
    folder = manifest.folders.get(folder_name)
    if not folder:
        raise ValueError(f"Folder '{folder_name}' not found in manifest")
    mapping = folder.mappings.get(mapping_name)
    if not mapping:
        raise ValueError(f"Mapping '{mapping_name}' not found in folder '{folder_name}'")

    if mapping.target_platform != TargetPlatform.DATABRICKS:
        return NotebookGenerationResult(
            mapping_name=mapping_name,
            folder=folder_name,
            skipped=True,
            skip_reason=mapping.target_platform.value if mapping.target_platform else "UNKNOWN",
        )

    sp = _load_session_params(params_dir, folder_name, mapping_name) if params_dir else {}
    return _generate_mapping(
        mapping, Path(output_dir),
        folder_sources=folder.sources,
        folder_targets=folder.targets,
        session_params=sp,
    )


# ---------------------------------------------------------------------------
# Report writer
# ---------------------------------------------------------------------------

def _write_report(report: GenerationReport, output_dir: Path) -> None:
    path = output_dir / "databricks-generation-report.json"

    def _serial(obj):
        if hasattr(obj, "__dataclass_fields__"):
            return asdict(obj)
        return str(obj)

    output_dir.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(asdict(report), fh, indent=2)
    log.info("Generation report -> %s", path)


# ---------------------------------------------------------------------------
# Manifest loader (reconstructs nested dataclasses from JSON dict)
# Same pattern as glue_gen/cli.py _load_manifest
# ---------------------------------------------------------------------------

def _load_manifest(data: dict) -> MigrationManifest:
    """Reconstruct a MigrationManifest from saved JSON dict."""

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
        m.connectors = [ConnectorDef(**c) for c in d.get("connectors", [])]
        m.variables = [
            MappingVariableDef(**{k: v for k, v in v.items()
                                  if k in MappingVariableDef.__dataclass_fields__})
            for v in d.get("variables", [])
        ]
        m.instances = [
            InstanceDef(**{k: v for k, v in i.items()
                           if k in InstanceDef.__dataclass_fields__})
            for i in d.get("instances", [])
        ]
        return m

    def _load_folder(d) -> FolderDef:
        f = FolderDef(name=d["name"], description=d.get("description", ""))
        f.sources = {k: _load_source(v) for k, v in d.get("sources", {}).items()}
        f.targets = {k: _load_target(v) for k, v in d.get("targets", {}).items()}
        f.mappings = {k: _load_mapping(v) for k, v in d.get("mappings", {}).items()}
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
