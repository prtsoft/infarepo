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
from .tf_builder import build_terraform_job, build_terraform_variables

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
    except Exception as exc:
        log.error("  [ERROR] %s / %s: %s", mapping.folder, mapping.name, exc)
        result.status = "ERROR"
        result.error = str(exc)
        return result

    # --- Terraform ---
    try:
        # Collect job args from the script for TF default_arguments
        job_args = _extract_args_from_script(script_text)
        tf_text = build_terraform_job(mapping, job_args)

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

def generate_all(
    manifest: MigrationManifest,
    output_dir: Path,
    folder_filter: Optional[List[str]] = None,
    include_review: bool = False,
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

            result = generate_mapping(mapping, folder, output_dir)
            folder_results.append(result)
            report.results.append(result)

            if result.status == "SUCCESS":
                report.generated += 1
            else:
                report.errors += 1

        # Generate shared variables.tf for this folder
        if folder_results:
            _generate_folder_variables(folder_name, folder_results, output_dir)

    _write_report(report, output_dir)
    return report


def generate_single(
    manifest: MigrationManifest,
    folder_name: str,
    mapping_name: str,
    output_dir: Path,
) -> MappingGenerationResult:
    """Generate a single mapping by name."""
    folder = manifest.folders.get(folder_name)
    if not folder:
        raise ValueError(f"Folder '{folder_name}' not found in manifest")
    mapping = folder.mappings.get(mapping_name)
    if not mapping:
        raise ValueError(f"Mapping '{mapping_name}' not found in folder '{folder_name}'")
    return generate_mapping(mapping, folder, Path(output_dir))


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
