"""
workflow_gen generator — Generate workflow orchestration artifacts from a manifest.

Targets:
  step-functions  — AWS Step Functions State Machine JSON + Terraform EventBridge trigger
  glue-workflow   — AWS Glue Workflow + Trigger Terraform HCL
  airflow         — Apache Airflow 2.x DAG Python files
  stub            — Minimal placeholder for unsupported orchestrators

Output layout:
  <output_dir>/
    workflows/
      <FOLDER>/
        <WORKFLOW_NAME>.json       (step-functions)
        <WORKFLOW_NAME>.tf         (step-functions Terraform or glue-workflow)
        <WORKFLOW_NAME>_dag.py     (airflow)
    workflow-generation-report.json
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

from pc_extractor.models import MigrationManifest, WorkflowDef

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Report types
# ---------------------------------------------------------------------------

@dataclass
class WorkflowGenerationResult:
    folder: str
    workflow: str
    target: str
    output_paths: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""
    error: Optional[str] = None


@dataclass
class WorkflowGenerationReport:
    generated_at: str
    target: str
    total: int = 0
    generated: int = 0
    skipped: int = 0
    errors: int = 0
    results: List[WorkflowGenerationResult] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Per-workflow generator dispatch
# ---------------------------------------------------------------------------

def _generate_workflow(
    workflow: WorkflowDef,
    output_dir: Path,
    target: str,
    generation_report: Optional[dict] = None,
) -> WorkflowGenerationResult:
    result = WorkflowGenerationResult(
        folder=workflow.folder,
        workflow=workflow.name,
        target=target,
    )

    wf_dir = output_dir / "workflows" / workflow.folder
    wf_dir.mkdir(parents=True, exist_ok=True)

    try:
        if target == "step-functions":
            _generate_step_functions(workflow, wf_dir, generation_report, result)
        elif target == "glue-workflow":
            _generate_glue_workflow(workflow, wf_dir, result)
        elif target == "airflow":
            _generate_airflow(workflow, wf_dir, generation_report, result)
        else:
            _generate_stub(workflow, wf_dir, result)
    except Exception as exc:
        log.error("  [ERROR] %s / %s: %s", workflow.folder, workflow.name, exc)
        result.error = str(exc)

    return result


def _generate_step_functions(
    workflow: WorkflowDef,
    out_dir: Path,
    generation_report: Optional[dict],
    result: WorkflowGenerationResult,
) -> None:
    from .step_functions_builder import build_step_functions
    import textwrap

    sm_json = build_step_functions(workflow, generation_report)

    # Write ASL JSON
    asl_path = out_dir / f"{workflow.name}.asl.json"
    asl_path.write_text(sm_json, encoding="utf-8")
    result.output_paths.append(str(asl_path))

    # Write Terraform resource
    from .schedule_translator import translate_schedule
    cron = translate_schedule(workflow.scheduler)
    safe_wf = workflow.name.lower().replace(" ", "_").replace("-", "_")
    safe_folder = workflow.folder.lower()

    tf_text = textwrap.dedent(f"""\
        # ---------------------------------------------------------------
        # Step Functions State Machine: {workflow.name}
        # Folder: {workflow.folder}
        # Migrated from Informatica PowerCenter 10.5
        # ---------------------------------------------------------------

        resource "aws_sfn_state_machine" "{safe_wf}" {{
          name     = "${{var.environment}}-{workflow.name.lower()}"
          role_arn = var.sfn_role_arn
          definition = file("${{path.module}}/{workflow.name}.asl.json")

          tags = {{
            ManagedBy    = "terraform"
            Environment  = var.environment
            SourceFolder = "{workflow.folder}"
            MigratedFrom = "InformaticaPowerCenter"
          }}
        }}

        output "{safe_wf}_arn" {{
          value = aws_sfn_state_machine.{safe_wf}.arn
        }}
    """)

    if not cron.startswith("#"):
        tf_text += textwrap.dedent(f"""\

            resource "aws_cloudwatch_event_rule" "{safe_wf}_schedule" {{
              name                = "${{var.environment}}-{workflow.name.lower()}-schedule"
              schedule_expression = "cron({cron})"
            }}

            resource "aws_cloudwatch_event_target" "{safe_wf}_target" {{
              rule      = aws_cloudwatch_event_rule.{safe_wf}_schedule.name
              target_id = "{safe_wf}"
              arn       = aws_sfn_state_machine.{safe_wf}.arn
              role_arn  = var.sfn_role_arn
            }}
        """)

    tf_path = out_dir / f"{workflow.name}.tf"
    tf_path.write_text(tf_text, encoding="utf-8")
    result.output_paths.append(str(tf_path))
    log.info("  [SFN] %-50s  tasks=%d", workflow.name, len(workflow.tasks))


def _generate_glue_workflow(
    workflow: WorkflowDef,
    out_dir: Path,
    result: WorkflowGenerationResult,
) -> None:
    from .glue_workflow_builder import build_glue_workflow

    tf_text = build_glue_workflow(workflow)
    tf_path = out_dir / f"{workflow.name}.tf"
    tf_path.write_text(tf_text, encoding="utf-8")
    result.output_paths.append(str(tf_path))
    log.info("  [GLUE-WF] %-50s  tasks=%d", workflow.name, len(workflow.tasks))


def _generate_airflow(
    workflow: WorkflowDef,
    out_dir: Path,
    generation_report: Optional[dict],
    result: WorkflowGenerationResult,
) -> None:
    from .airflow_builder import build_airflow_dag

    dag_text = build_airflow_dag(workflow)
    dag_path = out_dir / f"{workflow.name}_dag.py"
    dag_path.write_text(dag_text, encoding="utf-8")
    result.output_paths.append(str(dag_path))
    log.info("  [AIRFLOW] %-50s  tasks=%d", workflow.name, len(workflow.tasks))


def _generate_stub(
    workflow: WorkflowDef,
    out_dir: Path,
    result: WorkflowGenerationResult,
) -> None:
    stub_text = (
        f"# Stub workflow: {workflow.name}\n"
        f"# Folder: {workflow.folder}\n"
        f"# Tasks: {[t.name for t in workflow.tasks]}\n"
        f"# TODO: Implement orchestration for target platform\n"
    )
    stub_path = out_dir / f"{workflow.name}_stub.txt"
    stub_path.write_text(stub_text, encoding="utf-8")
    result.output_paths.append(str(stub_path))
    result.warnings.append("Generated stub only — choose a target platform.")
    log.info("  [STUB] %-50s", workflow.name)


# ---------------------------------------------------------------------------
# Session-to-job wiring
# ---------------------------------------------------------------------------

def _load_generation_report(report_path: Optional[Path]) -> Optional[dict]:
    if not report_path or not report_path.exists():
        return None
    try:
        return json.loads(report_path.read_text(encoding="utf-8"))
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def generate_all(
    manifest: MigrationManifest,
    output_dir: Path,
    target: str = "stub",
    folder_filter: Optional[List[str]] = None,
    generation_report_path: Optional[Path] = None,
) -> WorkflowGenerationReport:
    """
    Generate workflow orchestration for all workflows in the manifest.

    Args:
        manifest:                Parsed migration manifest.
        output_dir:              Root output directory.
        target:                  Orchestration target: step-functions | glue-workflow | airflow | stub
        folder_filter:           Only process these folders.
        generation_report_path:  Path to generation-report.json from glue-gen / databricks-gen.
                                 Used to wire session task names to actual job names.
    """
    output_dir = Path(output_dir)
    generation_report = _load_generation_report(generation_report_path)

    report = WorkflowGenerationReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        target=target,
    )

    for folder_name, folder in manifest.folders.items():
        if folder_filter and folder_name not in folder_filter:
            continue

        for workflow in folder.workflows.values():
            report.total += 1

            if not workflow.is_enabled:
                log.debug("Skipping disabled workflow %s/%s", folder_name, workflow.name)
                result = WorkflowGenerationResult(
                    folder=folder_name, workflow=workflow.name, target=target,
                    skipped=True, skip_reason="disabled",
                )
                report.skipped += 1
                report.results.append(result)
                continue

            result = _generate_workflow(workflow, output_dir, target, generation_report)
            report.results.append(result)
            if result.error:
                report.errors += 1
            else:
                report.generated += 1

    _write_report(report, output_dir)
    log.info(
        "Workflow summary: generated=%d  skipped=%d  errors=%d  total=%d",
        report.generated, report.skipped, report.errors, report.total,
    )
    return report


def generate_single(
    manifest: MigrationManifest,
    folder_name: str,
    workflow_name: str,
    output_dir: Path,
    target: str = "stub",
    generation_report_path: Optional[Path] = None,
) -> WorkflowGenerationResult:
    """Generate a single workflow by folder and name."""
    folder = manifest.folders.get(folder_name)
    if not folder:
        raise ValueError(f"Folder '{folder_name}' not found in manifest")
    workflow = folder.workflows.get(workflow_name)
    if not workflow:
        raise ValueError(f"Workflow '{workflow_name}' not found in folder '{folder_name}'")

    generation_report = _load_generation_report(generation_report_path)
    return _generate_workflow(workflow, Path(output_dir), target, generation_report)


def _write_report(report: WorkflowGenerationReport, output_dir: Path) -> None:
    path = output_dir / "workflow-generation-report.json"
    output_dir.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as fh:
        json.dump(asdict(report), fh, indent=2)
    log.info("Workflow generation report → %s", path)
