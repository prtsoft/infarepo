"""
IAM role + least-privilege policy builder for Glue jobs.

Generates terraform/iam/glue_role.tf with:
  - aws_iam_role for Glue jobs
  - aws_iam_role_policy (inline) scoped to source/target DB types:
      - S3 read/write on the job scripts and data buckets
      - Glue Catalog read (GetDatabase, GetTable, GetPartitions)
      - Secrets Manager read for connection ARNs
      - CloudWatch Logs write for job logging
      - KMS decrypt for SSM/S3 encrypted parameters (optional)
"""

from __future__ import annotations

import textwrap
from typing import List, Set

from .code_builder import _safe_var


# ---------------------------------------------------------------------------
# Policy statement builders
# ---------------------------------------------------------------------------

def _s3_policy(folder_name: str) -> str:
    return textwrap.dedent(f"""\
        {{
          "Sid": "S3JobBucketAccess",
          "Effect": "Allow",
          "Action": [
            "s3:GetObject",
            "s3:PutObject",
            "s3:DeleteObject",
            "s3:ListBucket"
          ],
          "Resource": [
            "arn:aws:s3:::${{var.scripts_bucket}}",
            "arn:aws:s3:::${{var.scripts_bucket}}/*",
            "arn:aws:s3:::${{var.logs_bucket}}",
            "arn:aws:s3:::${{var.logs_bucket}}/*",
            "arn:aws:s3:::${{var.temp_bucket}}",
            "arn:aws:s3:::${{var.temp_bucket}}/*"
          ]
        }}\
    """)


def _glue_catalog_policy() -> str:
    return textwrap.dedent("""\
        {
          "Sid": "GlueCatalogRead",
          "Effect": "Allow",
          "Action": [
            "glue:GetDatabase",
            "glue:GetDatabases",
            "glue:GetTable",
            "glue:GetTables",
            "glue:GetPartition",
            "glue:GetPartitions",
            "glue:BatchGetPartition"
          ],
          "Resource": ["*"]
        }\
    """)


def _secrets_manager_policy() -> str:
    return textwrap.dedent("""\
        {
          "Sid": "SecretsManagerRead",
          "Effect": "Allow",
          "Action": [
            "secretsmanager:GetSecretValue",
            "secretsmanager:DescribeSecret"
          ],
          "Resource": [
            "arn:aws:secretsmanager:*:*:secret:${var.environment}-glue-*"
          ]
        }\
    """)


def _cloudwatch_policy() -> str:
    return textwrap.dedent("""\
        {
          "Sid": "CloudWatchLogs",
          "Effect": "Allow",
          "Action": [
            "logs:CreateLogGroup",
            "logs:CreateLogStream",
            "logs:PutLogEvents"
          ],
          "Resource": ["arn:aws:logs:*:*:log-group:/aws-glue/*"]
        }\
    """)


def _ssm_policy() -> str:
    return textwrap.dedent("""\
        {
          "Sid": "SSMParameterRead",
          "Effect": "Allow",
          "Action": [
            "ssm:GetParameter",
            "ssm:GetParameters",
            "ssm:GetParametersByPath"
          ],
          "Resource": [
            "arn:aws:ssm:*:*:parameter/${var.environment}/*"
          ]
        }\
    """)


def _jdbc_network_policy(db_types: Set[str]) -> str:
    """For SQL Server / Oracle sources: allow VPC + EC2 describe for JDBC connections."""
    if not db_types.intersection({"SQLSERVER", "ORACLE", "MYSQL", "POSTGRESQL"}):
        return ""
    return textwrap.dedent("""\
        {
          "Sid": "GlueConnectionVPC",
          "Effect": "Allow",
          "Action": [
            "ec2:DescribeVpcEndpoints",
            "ec2:DescribeRouteTables",
            "ec2:CreateNetworkInterface",
            "ec2:DeleteNetworkInterface",
            "ec2:DescribeNetworkInterfaces",
            "ec2:DescribeSecurityGroups",
            "ec2:DescribeSubnets",
            "ec2:DescribeVpcAttribute",
            "ec2:CreateTags"
          ],
          "Resource": ["*"]
        }\
    """)


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_glue_iam_role(
    folder_name: str,
    source_db_types: List[str] = None,
    target_db_types: List[str] = None,
) -> str:
    """
    Generate terraform/iam/glue_role.tf with least-privilege IAM policy.

    Args:
        folder_name:     PC folder name, used for naming/tagging.
        source_db_types: DB types found in mapping sources (e.g. ["SQLSERVER", "ORACLE"]).
        target_db_types: DB types found in mapping targets (e.g. ["S3", "DELTA"]).
    """
    safe_folder = _safe_var(folder_name)
    all_db_types: Set[str] = set(source_db_types or []) | set(target_db_types or [])

    statements = [
        _s3_policy(folder_name),
        _glue_catalog_policy(),
        _secrets_manager_policy(),
        _cloudwatch_policy(),
        _ssm_policy(),
    ]

    jdbc_stmt = _jdbc_network_policy(all_db_types)
    if jdbc_stmt:
        statements.append(jdbc_stmt)

    statements_block = ",\n    ".join(statements)

    return textwrap.dedent(f"""\
        # ---------------------------------------------------------------
        # IAM Role for Glue jobs in folder: {folder_name}
        # Generated by pc-extractor glue-gen
        # Source DB types: {sorted(all_db_types)}
        # ---------------------------------------------------------------

        resource "aws_iam_role" "glue_{safe_folder}" {{
          name = "${{var.environment}}-glue-{safe_folder}-role"

          assume_role_policy = jsonencode({{
            Version   = "2012-10-17"
            Statement = [{{
              Action    = "sts:AssumeRole"
              Effect    = "Allow"
              Principal = {{
                Service = "glue.amazonaws.com"
              }}
            }}]
          }})

          tags = {{
            ManagedBy    = "terraform"
            Environment  = var.environment
            SourceFolder = "{folder_name}"
            MigratedFrom = "InformaticaPowerCenter"
          }}
        }}

        resource "aws_iam_role_policy" "glue_{safe_folder}_policy" {{
          name = "glue-{safe_folder}-least-privilege"
          role = aws_iam_role.glue_{safe_folder}.id

          policy = jsonencode({{
            Version   = "2012-10-17"
            Statement = [
              {statements_block}
            ]
          }})
        }}

        output "glue_{safe_folder}_role_arn" {{
          description = "Glue IAM role ARN for folder {folder_name}"
          value       = aws_iam_role.glue_{safe_folder}.arn
        }}
    """)
