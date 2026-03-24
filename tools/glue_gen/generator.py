"""
Top-level orchestrator for glue-gen.

For each GLUE-routed mapping:
  1. Build PySpark Glue script (code_builder)
  2. Build Terraform HCL (tf_builder)
  3. Write files to output directory

Output layout:
  <output_dir>/
    glue_jobs/
      <FOLDER>/
        <MAPPING_NAME>.py
    terraform/
      <FOLDER>/
        <MAPPING_NAME>.tf
        variables.tf           (one per folder, includes all connection vars)
    generation-report.json     (per-mapping success/warning/skip summary)
"""

from __future__ import annotations
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pc_extractor.models import FolderDef, MappingDef, MigrationManifest, TargetPlatform
from .code_builder import build_glue_script, _arg_name, _safe_var
from .tf_builder import (
    build_terraform_job,
    build_terraform_variables,
    build_glue_job_module,
    build_environment_main,
    build_environment_tfvars,
    build_environment_variables,
    build_backend_tf,
)
from .iam_builder import build_glue_iam_role


# ---------------------------------------------------------------------------
# Session param loader
# ---------------------------------------------------------------------------

def _load_session_params(params_dir: Path, folder_name: str, mapping_name: str) -> dict:
    """
    Load param-translator output for a specific folder+mapping.

    Searches `params_dir/glue-params/<folder>/` for JSON files whose
    _metadata.section task/session component matches the mapping name.
    Falls back to the merged view if no session-specific file is found.

    Returns a flat dict: { "PARAM_NAME": {"value": ..., "type": ..., "spark_value": ...} }
    """
    if not params_dir:
        return {}

    glue_params_dir = Path(params_dir) / "glue-params" / folder_name
    if not glue_params_dir.is_dir():
        return {}

    # 1. Look for <WORKFLOW>.<MAPPING_NAME>.json (session-specific file)
    mapping_upper = mapping_name.upper()
    for json_file in glue_params_dir.glob("*.json"):
        stem_parts = json_file.stem.upper().split(".")
        if len(stem_parts) >= 2 and stem_parts[-1] == mapping_upper:
            try:
                import json as _json
                data = _json.loads(json_file.read_text(encoding="utf-8"))
                return {k: v for k, v in data.items() if not k.startswith("_")}
            except Exception:
                pass

    # 2. Check each workflow JSON file's _metadata.section for the mapping name
    for json_file in glue_params_dir.glob("*.json"):
        try:
            import json as _json
            data = _json.loads(json_file.read_text(encoding="utf-8"))
            section = data.get("_metadata", {}).get("section", "")
            # section format: FOLDER.WORKFLOW:SESSION
            task_part = section.split(":")[-1].upper() if ":" in section else ""
            if task_part == mapping_upper:
                return {k: v for k, v in data.items() if not k.startswith("_")}
        except Exception:
            pass

    # 3. Fall back to _merged view if present
    merged_dir = Path(params_dir) / "glue-params" / "_merged"
    if merged_dir.is_dir():
        merged_files = list(merged_dir.glob("*.json"))
        if merged_files:
            try:
                import json as _json
                data = _json.loads(merged_files[0].read_text(encoding="utf-8"))
                return {k: v for k, v in data.items() if not k.startswith("_")}
            except Exception:
                pass

    return {}

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------

@dataclass
class MappingGenerationResult:
    folder: str
    mapping: str
    status: str          # SUCCESS, SKIPPED, ERROR
    glue_script_path: Optional[str] = None
    terraform_path: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    error: Optional[str] = None
    complexity_score: int = 0
    target_platform: str = ""


@dataclass
class GenerationReport:
    generated_at: str
    total_mappings: int = 0
    generated: int = 0
    skipped: int = 0
    errors: int = 0
    results: List[MappingGenerationResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Per-mapping generator
# ---------------------------------------------------------------------------

def generate_mapping(
    mapping: MappingDef,
    folder: FolderDef,
    output_dir: Path,
    session_params: Optional[dict] = None,
) -> MappingGenerationResult:
    result = MappingGenerationResult(
        folder=mapping.folder,
        mapping=mapping.name,
        status="SUCCESS",
        complexity_score=mapping.complexity_score or 0,
        target_platform=mapping.target_platform.value if mapping.target_platform else "",
    )

    # --- Glue script ---
    try:
        script_text, warnings = build_glue_script(mapping, folder)
        result.warnings = warnings

        glue_dir = output_dir / "glue_jobs" / mapping.folder
        glue_dir.mkdir(parents=True, exist_ok=True)
        script_path = glue_dir / f"{mapping.name}.py"
        script_path.write_text(script_text, encoding="utf-8")
        result.glue_script_path = str(script_path)
        log.info("  [GLUE] %-50s  warnings=%d", mapping.name, len(warnings))

        # Inline ruff lint (syntax errors only — E9, F rules)
        try:
            import subprocess as _sp
            ruff = _sp.run(
                ["ruff", "check", "--select=E9,F", "--output-format=text", str(script_path)],
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
        result.status = "ERROR"
        result.error = str(exc)
        return result

    # --- Terraform ---
    try:
        # Collect job args from the script for TF default_arguments
        job_args = _extract_args_from_script(script_text)
        tf_text = build_terraform_job(mapping, job_args, session_params=session_params or {})

        tf_dir = output_dir / "terraform" / mapping.folder
        tf_dir.mkdir(parents=True, exist_ok=True)
        tf_path = tf_dir / f"{mapping.name}.tf"
        tf_path.write_text(tf_text, encoding="utf-8")
        result.terraform_path = str(tf_path)
        log.info("  [TF]   %-50s", mapping.name)
    except Exception as exc:
        log.warning("  [WARN] Terraform generation failed for %s: %s", mapping.name, exc)
        result.warnings.append(f"Terraform generation failed: {exc}")

    return result


def _extract_args_from_script(script_text: str) -> List[str]:
    """Pull arg names from getResolvedOptions call in generated script."""
    import re
    # Match quoted strings inside getResolvedOptions
    block_match = re.search(
        r"getResolvedOptions\s*\(\s*sys\.argv\s*,\s*\[(.*?)\]",
        script_text,
        re.DOTALL,
    )
    if not block_match:
        return ["JOB_NAME"]
    raw = block_match.group(1)
    return [m.strip("'\" ") for m in re.findall(r"['\"][A-Z_][A-Z0-9_]*['\"]", raw)]


# ---------------------------------------------------------------------------
# Variables.tf — one per folder (idempotent)
# ---------------------------------------------------------------------------

def _generate_folder_iam(
    folder_name: str,
    folder: FolderDef,
    output_dir: Path,
) -> None:
    """Generate terraform/iam/glue_role.tf for a folder."""
    source_db_types: List[str] = []
    target_db_types: List[str] = []
    for mapping in folder.mappings.values():
        if mapping.flags:
            source_db_types.extend(mapping.flags.source_db_types or [])
            target_db_types.extend(mapping.flags.target_db_types or [])

    iam_dir = output_dir / "terraform" / "iam"
    iam_dir.mkdir(parents=True, exist_ok=True)
    iam_path = iam_dir / f"glue_{folder_name.lower()}_role.tf"
    iam_path.write_text(
        build_glue_iam_role(folder_name, source_db_types, target_db_types),
        encoding="utf-8",
    )
    log.info("  [IAM]  %s", iam_path)


def _generate_folder_variables(
    folder_name: str,
    results: List[MappingGenerationResult],
    output_dir: Path,
) -> None:
    """Generate shared variables.tf for a folder (overwrites each time)."""
    # Collect all unique connection names across all mappings in this folder
    conn_names: List[str] = []
    for r in results:
        if r.terraform_path:
            tf_text = Path(r.terraform_path).read_text(encoding="utf-8")
            import re
            for m in re.finditer(r'"connection_([a-z0-9_]+)"', tf_text):
                name = m.group(1)
                if name not in conn_names:
                    conn_names.append(name)

    vars_text = build_terraform_variables(folder_name, conn_names)
    tf_dir = output_dir / "terraform" / folder_name
    tf_dir.mkdir(parents=True, exist_ok=True)
    (tf_dir / "variables.tf").write_text(vars_text, encoding="utf-8")


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def _generate_env_split(
    folder_name: str,
    mapping_names: List[str],
    output_dir: Path,
    intake: Optional[dict] = None,
) -> None:
    """Write modules/ + environments/ layout for multi-env Terraform."""
    intake = intake or {}
    aws_accounts: dict = intake.get("aws_accounts", {})
    aws_region: str = intake.get("aws_region", "us-east-1")
    tf_state: dict = intake.get("terraform_state", {})
    s3_bucket    = tf_state.get("s3_bucket", "")
    dynamo_table = tf_state.get("dynamodb_table", "")

    # Module definition
    mod_dir = output_dir / "terraform" / "modules" / "glue_job"
    mod_dir.mkdir(parents=True, exist_ok=True)
    (mod_dir / "main.tf").write_text(build_glue_job_module(), encoding="utf-8")

    envs = list(aws_accounts.keys()) if aws_accounts else ["dev", "staging", "prod"]

    for env in envs:
        env_data = aws_accounts.get(env, {})
        account_id = env_data.get("account_id", "")
        region     = env_data.get("region", aws_region)
        scripts_b  = env_data.get("scripts_bucket", "")
        logs_b     = env_data.get("logs_bucket", "")
        temp_b     = env_data.get("temp_bucket", "")

        env_dir = output_dir / "terraform" / "environments" / env
        env_dir.mkdir(parents=True, exist_ok=True)

        (env_dir / "main.tf").write_text(
            build_environment_main(folder_name, mapping_names, env, account_id, region),
            encoding="utf-8",
        )
        (env_dir / "variables.tf").write_text(
            build_environment_variables(folder_name),
            encoding="utf-8",
        )
        (env_dir / "terraform.tfvars").write_text(
            build_environment_tfvars(folder_name, env, account_id, region, scripts_b, logs_b, temp_b),
            encoding="utf-8",
        )
        (env_dir / "backend.tf").write_text(
            build_backend_tf(env, s3_bucket, dynamo_table, region, folder_name),
            encoding="utf-8",
        )
    log.info("  [ENV-SPLIT] %s → %d environments", folder_name, len(envs))


def generate_all(
    manifest: MigrationManifest,
    output_dir: Path,
    folder_filter: Optional[List[str]] = None,
    include_review: bool = False,
    params_dir: Optional[Path] = None,
    env_split: bool = False,
    intake: Optional[dict] = None,
) -> GenerationReport:
    """
    Generate Glue scripts + Terraform for all GLUE-routed mappings in the manifest.

    Args:
        manifest:       Parsed + scored migration manifest
        output_dir:     Root output directory
        folder_filter:  If set, only process these folder names
        include_review: If True, also generate stubs for REVIEW-routed mappings
    """
    output_dir = Path(output_dir)
    report = GenerationReport(
        generated_at=datetime.now(timezone.utc).isoformat()
    )

    for folder_name, folder in manifest.folders.items():
        if folder_filter and folder_name not in folder_filter:
            continue

        folder_results: List[MappingGenerationResult] = []

        for mapping in folder.mappings.values():
            report.total_mappings += 1
            platform = mapping.target_platform

            if platform == TargetPlatform.DATABRICKS:
                log.debug("Skipping %s/%s (DATABRICKS)", folder_name, mapping.name)
                result = MappingGenerationResult(
                    folder=folder_name,
                    mapping=mapping.name,
                    status="SKIPPED",
                    target_platform="DATABRICKS",
                )
                report.skipped += 1
                report.results.append(result)
                continue

            if platform == TargetPlatform.REVIEW and not include_review:
                log.debug("Skipping %s/%s (REVIEW)", folder_name, mapping.name)
                result = MappingGenerationResult(
                    folder=folder_name,
                    mapping=mapping.name,
                    status="SKIPPED",
                    target_platform="REVIEW",
                    warnings=mapping.review_notes,
                )
                report.skipped += 1
                report.results.append(result)
                continue

            sp = _load_session_params(params_dir, folder_name, mapping.name) if params_dir else {}
            result = generate_mapping(mapping, folder, output_dir, session_params=sp)
            folder_results.append(result)
            report.results.append(result)

            if result.status == "SUCCESS":
                report.generated += 1
            else:
                report.errors += 1

        # Generate shared variables.tf for this folder
        if folder_results:
            _generate_folder_variables(folder_name, folder_results, output_dir)
            _generate_folder_iam(folder_name, folder, output_dir)
            if env_split:
                names = [r.mapping for r in folder_results if r.status == "SUCCESS"]
                _generate_env_split(folder_name, names, output_dir, intake=intake)

    _write_report(report, output_dir)
    return report


def generate_single(
    manifest: MigrationManifest,
    folder_name: str,
    mapping_name: str,
    output_dir: Path,
    params_dir: Optional[Path] = None,
) -> MappingGenerationResult:
    """Generate a single mapping by name."""
    folder = manifest.folders.get(folder_name)
    if not folder:
        raise ValueError(f"Folder '{folder_name}' not found in manifest")
    mapping = folder.mappings.get(mapping_name)
    if not mapping:
        raise ValueError(f"Mapping '{mapping_name}' not found in folder '{folder_name}'")
    sp = _load_session_params(params_dir, folder_name, mapping_name) if params_dir else {}
    return generate_mapping(mapping, folder, Path(output_dir), session_params=sp)


def _write_report(report: GenerationReport, output_dir: Path) -> None:
    path = output_dir / "generation-report.json"

    def _serial(obj):
        if hasattr(obj, "__dataclass_fields__"):
            return asdict(obj)
        return str(obj)

    with open(path, "w", encoding="utf-8") as fh:
        json.dump(report, fh, default=_serial, indent=2)
    log.info("Generation report → %s", path)
    log.info(
        "Summary: generated=%d  skipped=%d  errors=%d  total=%d",
        report.generated, report.skipped, report.errors, report.total_mappings,
    )
