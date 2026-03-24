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


def _default_arg_block(
    args: List[str],
    mapping: MappingDef,
    scripts_bucket_var: str,
    session_params: Optional[Dict[str, dict]] = None,
) -> str:
    """Build the default_arguments map.

    session_params: flat dict loaded from param-translator glue-params JSON.
    Format: { "PARAM_NAME": {"value": ..., "type": ..., "spark_value": ...} }
    When provided, actual values from the param file replace empty-string placeholders.
    """
    sp = session_params or {}

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

    # If session params provided, add PARAMS_S3_PATH so the runtime loader can find them
    if sp:
        lines.append(
            f'    "--PARAMS_S3_PATH"             = '
            f'"s3://${{var.scripts_bucket}}/params/{mapping.folder}/{mapping.name}.json"'
        )

    # Mapping parameter variables → job default args
    # Use actual values from session_params when available
    for var in mapping.variables:
        if var.is_param:
            arg_name = var.name.lstrip("$")
            if arg_name in sp:
                raw = sp[arg_name].get("value", "")
                ptype = sp[arg_name].get("type", "")
                comment = f"  # {ptype}" if ptype else ""
                default = str(raw) if raw is not None else ""
            else:
                default = var.default_value or ""
                comment = ""
            lines.append(f'    "--{arg_name}"' + " " * max(1, 30 - len(arg_name)) + f'= "{default}"{comment}')

    # Add any session params not already covered by mapping.variables
    already_added = {v.name.lstrip("$") for v in mapping.variables if v.is_param}
    for param_name, param_data in sp.items():
        if param_name not in already_added:
            raw = param_data.get("value", "")
            ptype = param_data.get("type", "")
            comment = f"  # {ptype}" if ptype else ""
            default = str(raw) if raw is not None else ""
            lines.append(f'    "--{param_name}"' + " " * max(1, 30 - len(param_name)) + f'= "{default}"{comment}')

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
    session_params: Optional[Dict[str, dict]] = None,
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

    default_args = _default_arg_block(job_args, mapping, scripts_bucket_var, session_params=session_params)

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
# Multi-environment support (--env-split)
# ---------------------------------------------------------------------------

def build_glue_job_module() -> str:
    """Reusable Terraform module definition for a Glue job (modules/glue_job/main.tf)."""
    return textwrap.dedent("""\
        # ---------------------------------------------------------------
        # Reusable module: modules/glue_job
        # Generated by pc-extractor glue-gen --env-split
        # ---------------------------------------------------------------

        variable "job_name"        { type = string }
        variable "script_location" { type = string }
        variable "role_arn"        { type = string }
        variable "environment"     { type = string }
        variable "scripts_bucket"  { type = string }
        variable "logs_bucket"     { type = string }
        variable "temp_bucket"     { type = string }
        variable "worker_type"     { type = string; default = "G.1X" }
        variable "num_workers"     { type = number; default = 2 }
        variable "folder"          { type = string }

        variable "default_arguments" {
          type    = map(string)
          default = {}
        }

        resource "aws_glue_job" "this" {
          name     = "${var.environment}-${var.job_name}"
          role_arn = var.role_arn

          command {
            name            = "glueetl"
            script_location = var.script_location
            python_version  = "3"
          }

          glue_version      = "4.0"
          worker_type       = var.worker_type
          number_of_workers = var.num_workers
          timeout           = 2880

          default_arguments = merge({
            "--job-language"                     = "python"
            "--enable-metrics"                   = "true"
            "--enable-spark-ui"                  = "true"
            "--spark-event-logs-path"            = "s3://${var.logs_bucket}/spark-logs/${var.folder}/"
            "--TempDir"                          = "s3://${var.temp_bucket}/temp/${var.folder}/"
            "--enable-glue-datacatalog"          = "true"
            "--job-bookmark-option"              = "job-bookmark-enable"
            "--enable-continuous-cloudwatch-log" = "true"
          }, var.default_arguments)

          execution_property {
            max_concurrent_runs = 1
          }

          tags = {
            ManagedBy       = "terraform"
            Environment     = var.environment
            SourceFolder    = var.folder
            MigratedFrom    = "InformaticaPowerCenter"
          }
        }

        output "job_name" {
          value = aws_glue_job.this.name
        }
    """)


def build_environment_main(
    folder_name: str,
    mapping_names: List[str],
    env: str,
    aws_account_id: str = "",
    aws_region: str = "us-east-1",
) -> str:
    """Generate environments/<env>/main.tf calling the shared module."""
    module_calls = []
    for name in mapping_names:
        safe = _safe_var(name)
        module_calls.append(textwrap.dedent(f"""\
            module "{safe}" {{
              source     = "../../modules/glue_job"
              job_name   = "{name.lower()}"
              folder     = "{folder_name}"
              environment     = var.environment
              role_arn        = var.glue_role_arn
              scripts_bucket  = var.scripts_bucket
              logs_bucket     = var.logs_bucket
              temp_bucket     = var.temp_bucket
              script_location = "s3://${{var.scripts_bucket}}/glue_jobs/{folder_name}/{name}.py"
            }}
        """))

    provider_block = textwrap.dedent(f"""\
        # ---------------------------------------------------------------
        # Environment: {env}  |  Folder: {folder_name}
        # Generated by pc-extractor glue-gen --env-split
        # ---------------------------------------------------------------

        terraform {{
          required_providers {{
            aws = {{
              source  = "hashicorp/aws"
              version = "~> 5.0"
            }}
          }}
        }}

        provider "aws" {{
          region = var.aws_region
        }}

    """)
    return provider_block + "\n".join(module_calls)


def build_environment_tfvars(
    folder_name: str,
    env: str,
    aws_account_id: str = "",
    aws_region: str = "us-east-1",
    scripts_bucket: str = "",
    logs_bucket: str = "",
    temp_bucket: str = "",
) -> str:
    """Generate environments/<env>/terraform.tfvars with environment-specific values."""
    scripts_b = scripts_bucket or f"{env}-glue-scripts"
    logs_b    = logs_bucket    or f"{env}-glue-logs"
    temp_b    = temp_bucket    or f"{env}-glue-temp"
    return textwrap.dedent(f"""\
        # ---------------------------------------------------------------
        # Terraform variable values for environment: {env}
        # Folder: {folder_name}
        # ---------------------------------------------------------------

        environment    = "{env}"
        aws_region     = "{aws_region}"
        scripts_bucket = "{scripts_b}"
        logs_bucket    = "{logs_b}"
        temp_bucket    = "{temp_b}"
        # glue_role_arn  = "arn:aws:iam::{aws_account_id or 'ACCOUNT_ID'}:role/{env}-glue-role"
    """)


def build_environment_variables(folder_name: str) -> str:
    """Generate environments/<env>/variables.tf with variable declarations."""
    return textwrap.dedent(f"""\
        # ---------------------------------------------------------------
        # Variable declarations for environment configs (folder: {folder_name})
        # ---------------------------------------------------------------

        variable "environment" {{
          type = string
        }}

        variable "aws_region" {{
          type    = string
          default = "us-east-1"
        }}

        variable "glue_role_arn" {{
          type = string
        }}

        variable "scripts_bucket" {{
          type = string
        }}

        variable "logs_bucket" {{
          type = string
        }}

        variable "temp_bucket" {{
          type = string
        }}
    """)


def build_backend_tf(
    env: str,
    s3_bucket: str = "",
    dynamodb_table: str = "",
    aws_region: str = "us-east-1",
    folder_name: str = "",
) -> str:
    """Generate backend.tf with S3 remote state + DynamoDB locking."""
    bucket  = s3_bucket       or "terraform-state-bucket"
    table   = dynamodb_table  or "terraform-state-lock"
    key     = f"glue/{folder_name}/{env}/terraform.tfstate" if folder_name else f"glue/{env}/terraform.tfstate"
    return textwrap.dedent(f"""\
        # ---------------------------------------------------------------
        # Terraform remote state — S3 backend with DynamoDB locking
        # Environment: {env}
        # ---------------------------------------------------------------

        terraform {{
          backend "s3" {{
            bucket         = "{bucket}"
            key            = "{key}"
            region         = "{aws_region}"
            encrypt        = true
            dynamodb_table = "{table}"
          }}
        }}
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
