"""
glue_workflow_builder.py — Build AWS Glue Workflow + Trigger Terraform HCL
from a WorkflowDef.

Generates:
  - aws_glue_workflow resource
  - aws_glue_trigger per link (conditional or on-demand)
  - EventBridge schedule trigger if workflow has a schedule
"""

from __future__ import annotations

import re
import textwrap
from typing import List, Optional

from pc_extractor.models import WorkflowDef, WorkflowTaskDef, WorkflowLinkDef
from .schedule_translator import translate_schedule


def _safe(name: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", name.lower()).strip("_")


def build_glue_workflow(workflow: WorkflowDef) -> str:
    """
    Build Terraform HCL for an aws_glue_workflow and its triggers.

    Returns HCL string.
    """
    safe_wf = _safe(workflow.name)
    blocks: List[str] = []

    # Workflow resource
    blocks.append(textwrap.dedent(f"""\
        # ---------------------------------------------------------------
        # Glue Workflow: {workflow.name}
        # Folder       : {workflow.folder}
        # Migrated from Informatica PowerCenter 10.5
        # ---------------------------------------------------------------

        resource "aws_glue_workflow" "{safe_wf}" {{
          name        = "${{var.environment}}-{workflow.name.lower()}"
          description = "{workflow.description or workflow.name}"

          tags = {{
            ManagedBy    = "terraform"
            Environment  = var.environment
            MigratedFrom = "InformaticaPowerCenter"
          }}
        }}
    """))

    # Schedule trigger (if workflow has schedule)
    cron = translate_schedule(workflow.scheduler)
    if not cron.startswith("#"):
        # EventBridge/Glue scheduled trigger
        blocks.append(textwrap.dedent(f"""\
            resource "aws_glue_trigger" "{safe_wf}_schedule" {{
              name          = "${{var.environment}}-{workflow.name.lower()}-schedule"
              type          = "SCHEDULED"
              schedule      = "{cron}"
              workflow_name = aws_glue_workflow.{safe_wf}.name

              actions {{
                job_name = aws_glue_job.{_safe(workflow.tasks[0].name if workflow.tasks else "start")}.name
              }}

              tags = {{
                ManagedBy   = "terraform"
                Environment = var.environment
              }}
            }}
        """))
    else:
        blocks.append(
            f"# Schedule: {cron}\n"
            f"# Add an aws_glue_trigger with type = \"SCHEDULED\" when schedule is confirmed.\n\n"
        )

    # On-demand trigger (start)
    if workflow.tasks:
        first_task = workflow.tasks[0]
        if first_task.mapping_ref:
            safe_job = _safe(first_task.mapping_ref)
            blocks.append(textwrap.dedent(f"""\
                resource "aws_glue_trigger" "{safe_wf}_on_demand" {{
                  name          = "${{var.environment}}-{workflow.name.lower()}-on-demand"
                  type          = "ON_DEMAND"
                  workflow_name = aws_glue_workflow.{safe_wf}.name

                  actions {{
                    job_name = aws_glue_job.{safe_job}.name
                  }}
                }}
            """))

    # Conditional triggers between tasks
    for link in workflow.links:
        from_safe = _safe(link.from_task)
        to_task_obj = next((t for t in workflow.tasks if t.name == link.to_task), None)
        if to_task_obj and to_task_obj.mapping_ref:
            to_safe = _safe(to_task_obj.mapping_ref)
            trigger_name = f"{safe_wf}_{from_safe}_to_{_safe(link.to_task)}"
            condition_block = ""
            if link.condition:
                escaped = link.condition.replace('"', '\\"')
                condition_block = textwrap.dedent(f"""\
                      predicate {{
                        conditions {{
                          job_name = aws_glue_job.{from_safe}.name
                          state    = "SUCCEEDED"
                          # Original condition: {escaped}
                        }}
                      }}
                """)

            blocks.append(textwrap.dedent(f"""\
                resource "aws_glue_trigger" "{trigger_name}" {{
                  name          = "${{var.environment}}-{trigger_name.replace('_', '-')}"
                  type          = "CONDITIONAL"
                  workflow_name = aws_glue_workflow.{safe_wf}.name
                {condition_block}
                  actions {{
                    job_name = aws_glue_job.{to_safe}.name
                  }}
                }}
            """))

    return "\n".join(blocks)
