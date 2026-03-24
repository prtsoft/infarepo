"""
Terraform HCL builder for AWS Glue jobs.

Generates:
  glue_jobs/{folder}/{mapping_name}.tf     — aws_glue_job resource
  glue_jobs/{folder}/variables.tf          — variable declarations (generated once per folder)

Design decisions:
  - One .tf file per mapping (easy to PR review, easy to destroy individual jobs)
  - Connection names are passed as Glue job default_arguments
  - S3 paths use variables (scripts_bucket, logs_bucket, temp_bucket)
  - Glue version 4.0 (Python 3.10, Spark 3.3)
  - G.1X worker type, 2 workers default (override via variable)
  - Job bookmark enabled by default (idempotency + HIPAA audit trail)
  - CloudWatch metrics enabled
  - Tags include: ManagedBy, SourceMapping, MigratedFrom, Environment
"""

from __future__ import annotations
import textwrap
from typing import Dict, List, Optional

from pc_extractor.models import MappingDef, MappingVariableDef
from .code_builder import _arg_name, _safe_var, _is_lakehouse


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _tf_resource_name(mapping_name: str) -> str:
    return _safe_var(mapping_name)


def _default_arg_block(args: List[str], mapping: MappingDef, scripts_bucket_var: str) -> str:
    """Build the default_arguments map."""
    lines = [
        '    "--job-language"                 = "python"',
        '    "--enable-metrics"               = "true"',
        '    "--enable-spark-ui"              = "true"',
        '    "--enable-job-insights"          = "true"',
        f'    "--spark-event-logs-path"        = "s3://${{var.logs_bucket}}/spark-logs/{mapping.folder}/"',
        f'    "--TempDir"                      = "s3://${{var.temp_bucket}}/temp/{mapping.folder}/"',
        '    "--enable-glue-datacatalog"      = "true"',
        '    "--job-bookmark-option"          = "job-bookmark-enable"',
        '    "--enable-continuous-cloudwatch-log" = "true"',
    ]

    # Mapping parameter variables → job default args (empty string default, override at runtime)
    for var in mapping.variables:
        if var.is_param:
            arg_name = var.name.lstrip("$")
            default = var.default_value or ""
            lines.append(f'    "--{arg_name}"' + " " * max(1, 30 - len(arg_name)) + f'= "{default}"')

    # Connection args — empty by default, must be overridden per environment
    for arg in args:
        if arg.startswith("CONN_"):
            lines.append(f'    "--{arg}"' + " " * max(1, 30 - len(arg)) + '= ""  # TODO: set connection name')

    # S3 path args
    if "S3_INPUT_PATH" in args:
        lines.append(f'    "--S3_INPUT_PATH"              = ""  # TODO: set input S3 path')
    if "S3_OUTPUT_PATH" in args:
        lines.append(f'    "--S3_OUTPUT_PATH"             = ""  # TODO: set output S3 path')

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Job resource
# ---------------------------------------------------------------------------

def build_terraform_job(
    mapping: MappingDef,
    job_args: List[str],
    scripts_bucket_var: str = "var.scripts_bucket",
) -> str:
    resource_name = _tf_resource_name(mapping.name)
    script_path = (
        f"s3://${{var.scripts_bucket}}/glue_jobs/{mapping.folder}/{mapping.name}.py"
    )
    platform = mapping.target_platform.value if mapping.target_platform else "GLUE"
    score = mapping.complexity_score or 1

    # Worker sizing hint based on complexity
    num_workers = 2 if score <= 4 else (5 if score <= 7 else 10)
    worker_type = "G.1X" if score <= 6 else "G.2X"

    # Connections used (unique connection args that have a CONN_ prefix)
    connection_vars = [
        f'var.connection_{_safe_var(a.replace("CONN_", "").replace("CONN_TGT_", "").replace("CONN_LKP_", ""))}'
        for a in job_args if a.startswith("CONN_")
    ]
    connections_block = ""
    if connection_vars:
        conn_list = "\n    ".join(f'"{c}",' for c in connection_vars)
        connections_block = f"\n  connections = [\n    {conn_list}\n  ]\n"

    default_args = _default_arg_block(job_args, mapping, scripts_bucket_var)

    return textwrap.dedent(f"""\
        # ---------------------------------------------------------------
        # Glue Job: {mapping.name}
        # Folder  : {mapping.folder}
        # Score   : {score}/10  |  Platform: {platform}
        # Migrated from Informatica PowerCenter 10.5
        # ---------------------------------------------------------------

        resource "aws_glue_job" "{resource_name}" {{
          name     = "${{var.environment}}-{mapping.name.lower()}"
          role_arn = var.glue_role_arn

          command {{
            name            = "glueetl"
            script_location = "{script_path}"
            python_version  = "3"
          }}

          glue_version      = "4.0"
          worker_type       = "{worker_type}"
          number_of_workers = {num_workers}
          timeout           = 2880   # 48 hours max; tune per job

          default_arguments = {{
        {default_args}
          }}
        {connections_block}
          execution_property {{
            max_concurrent_runs = 1
          }}

          tags = {{
            ManagedBy     = "terraform"
            Environment   = var.environment
            SourceMapping = "{mapping.name}"
            SourceFolder  = "{mapping.folder}"
            MigratedFrom  = "InformaticaPowerCenter"
            ComplexityScore = "{score}"
          }}
        }}

        output "{resource_name}_job_name" {{
          description = "Glue job name for {mapping.name}"
          value       = aws_glue_job.{resource_name}.name
        }}
    """)


# ---------------------------------------------------------------------------
# Shared variables file (one per folder, idempotent generation)
# ---------------------------------------------------------------------------

def build_terraform_variables(folder_name: str, connection_names: List[str]) -> str:
    conn_vars = ""
    for name in sorted(set(connection_names)):
        safe = _safe_var(name)
        conn_vars += textwrap.dedent(f"""\
            variable "connection_{safe}" {{
              description = "AWS Glue connection name for {name}"
              type        = string
            }}

        """)

    return textwrap.dedent(f"""\
        # ---------------------------------------------------------------
        # Shared variables for Glue jobs in folder: {folder_name}
        # Generated by pc-extractor glue-gen
        # ---------------------------------------------------------------

        variable "environment" {{
          description = "Deployment environment (dev / stage / prod)"
          type        = string
          validation {{
            condition     = contains(["dev", "stage", "prod"], var.environment)
            error_message = "environment must be dev, stage, or prod"
          }}
        }}

        variable "glue_role_arn" {{
          description = "IAM role ARN for Glue jobs in this folder"
          type        = string
        }}

        variable "scripts_bucket" {{
          description = "S3 bucket name where Glue scripts are stored"
          type        = string
        }}

        variable "logs_bucket" {{
          description = "S3 bucket name for Spark event logs and CloudWatch"
          type        = string
        }}

        variable "temp_bucket" {{
          description = "S3 bucket for Glue TempDir (shuffle, spill)"
          type        = string
        }}

        {conn_vars}
    """)


# ---------------------------------------------------------------------------
# EventBridge schedule (optional)
# ---------------------------------------------------------------------------

def build_eventbridge_schedule(
    mapping: MappingDef,
    workflow_name: str,
    cron_expr: str = "cron(0 2 * * ? *)",
) -> str:
    resource_name = _tf_resource_name(mapping.name)
    return textwrap.dedent(f"""\
        # EventBridge schedule for {mapping.name}
        # Original PC workflow: {workflow_name}
        # Original schedule: see migration notes

        resource "aws_scheduler_schedule" "{resource_name}_schedule" {{
          name       = "${{var.environment}}-{mapping.name.lower()}-schedule"
          group_name = "default"

          flexible_time_window {{
            mode = "OFF"
          }}

          schedule_expression = "{cron_expr}"

          target {{
            arn      = "arn:aws:glue:${{data.aws_region.current.name}}:${{data.aws_caller_identity.current.account_id}}:job/${{aws_glue_job.{resource_name}.name}}"
            role_arn = var.scheduler_role_arn

            glue_parameters {{
              job_name = aws_glue_job.{resource_name}.name
            }}
          }}

          tags = {{
            ManagedBy   = "terraform"
            Environment = var.environment
          }}
        }}
    """)
