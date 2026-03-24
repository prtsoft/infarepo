#!/usr/bin/env python3
"""
Informatica PowerCenter Migration Intake Tool
Run this script, answer the questions, and share the output JSON with your consultant.
"""

import json
import sys
import os
from datetime import datetime

def ask(question, options=None, multi=False, default=None):
    print()
    print(f"  {question}")
    if options:
        for i, opt in enumerate(options, 1):
            print(f"    {i}) {opt}")
        if multi:
            print("    (Enter comma-separated numbers, e.g. 1,3,4)")
        prompt = f"  > "
        if default:
            prompt = f"  > [{default}] "
        while True:
            val = input(prompt).strip()
            if not val and default:
                return default
            if not val:
                continue
            if multi:
                try:
                    indices = [int(x.strip()) - 1 for x in val.split(",")]
                    return [options[i] for i in indices if 0 <= i < len(options)]
                except:
                    print("  Invalid input, try again.")
                    continue
            else:
                try:
                    idx = int(val) - 1
                    if 0 <= idx < len(options):
                        return options[idx]
                    print("  Invalid selection, try again.")
                except:
                    # allow free text if no valid number
                    return val
    else:
        prompt = f"  > "
        if default:
            prompt = f"  > [{default}] "
        val = input(prompt).strip()
        return val if val else (default or "")

def ask_free(question, default=None):
    print()
    print(f"  {question}")
    prompt = f"  > "
    if default:
        prompt = f"  > [{default}] "
    val = input(prompt).strip()
    return val if val else (default or "")

def section(title):
    print()
    print("=" * 60)
    print(f"  {title}")
    print("=" * 60)

def main():
    print()
    print("=" * 60)
    print("  Informatica PowerCenter Migration Intake")
    print("  Fill this out to generate your migration-intake.json")
    print("=" * 60)
    print()
    print("  Answer each question. Press Enter to accept [defaults].")
    print("  This takes about 5-10 minutes.")

    intake = {"generated_at": datetime.now().isoformat()}

    # ----------------------------------------------------------------
    section("1 of 6 — PowerCenter Environment")
    # ----------------------------------------------------------------

    intake["pc_version"] = ask_free(
        "What version of PowerCenter are you on?",
        default="10.5"
    )

    intake["mapping_count"] = ask(
        "Roughly how many PowerCenter MAPPINGS are in scope?",
        options=["< 50", "50 – 200", "200 – 500", "500 – 1000", "1000 – 2000", "2000+"]
    )

    intake["workflow_count"] = ask(
        "Roughly how many WORKFLOWS (schedulable units)?",
        options=["< 25", "25 – 100", "100 – 300", "300 – 750", "750+"]
    )

    intake["complexity_mix"] = ask(
        "What's the rough complexity mix of your mappings?",
        options=[
            "Mostly simple (filter, join, lookup, expression, aggregator)",
            "Mix of simple and moderate (multi-source, union, router, normalizer)",
            "Many complex (dynamic mappings, stored procs, custom transforms, Java)",
            "Mostly complex"
        ]
    )

    intake["advanced_features"] = ask(
        "Do you use any of these advanced features? (pick all that apply)",
        options=[
            "Pushdown optimization (PDO)",
            "Dynamic mappings",
            "Parameter files / mapping variables",
            "Pre/post session SQL",
            "Stored procedure calls",
            "Custom / plug-in transformations",
            "Partitioning / grid execution",
            "Real-time / messaging (MQ, JMS)",
            "None of the above"
        ],
        multi=True
    )

    intake["metadata_access"] = ask(
        "How can you access PowerCenter metadata?",
        options=[
            "XML exports from Designer / Repository Manager",
            "Direct access to repository database (Oracle/SQL Server)",
            "pmrep command-line tool available",
            "Informatica Metadata Manager",
            "Not sure yet"
        ],
        multi=True
    )

    # ----------------------------------------------------------------
    section("2 of 6 — Source Systems")
    # ----------------------------------------------------------------

    intake["source_databases"] = ask(
        "Which DATABASE sources are in scope? (pick all that apply)",
        options=[
            "Oracle",
            "SQL Server",
            "PostgreSQL",
            "MySQL / MariaDB",
            "DB2",
            "Teradata",
            "Snowflake",
            "Redshift",
            "SAP HANA",
            "Other / not listed"
        ],
        multi=True
    )

    intake["source_files"] = ask(
        "Which FILE source types are in scope? (pick all that apply)",
        options=[
            "S3 flat files (CSV / delimited)",
            "Fixed-width flat files",
            "XML files",
            "JSON files",
            "SFTP / FTP files",
            "Mainframe / VSAM",
            "Excel",
            "None — no file sources"
        ],
        multi=True
    )

    intake["source_other"] = ask(
        "Any other source types? (pick all that apply)",
        options=[
            "REST / SOAP APIs",
            "Message queues (MQ, Kafka, Kinesis)",
            "SAP (BAPI / RFC)",
            "Salesforce",
            "None"
        ],
        multi=True
    )

    # ----------------------------------------------------------------
    section("3 of 6 — Target Systems")
    # ----------------------------------------------------------------

    intake["target_databases"] = ask(
        "Which DATABASE targets are in scope? (pick all that apply)",
        options=[
            "Amazon RDS (Oracle)",
            "Amazon RDS (SQL Server)",
            "Amazon RDS (PostgreSQL / MySQL)",
            "Amazon Redshift",
            "Snowflake",
            "Aurora",
            "DynamoDB",
            "On-prem Oracle / SQL Server (staying)",
            "Other"
        ],
        multi=True
    )

    intake["target_lakehouse"] = ask(
        "Which LAKEHOUSE / FILE targets are in scope? (pick all that apply)",
        options=[
            "S3 (raw / landing zone)",
            "S3 + Glue Catalog (Parquet / ORC)",
            "Delta Lake on S3 (Databricks)",
            "Apache Iceberg",
            "Hive Metastore",
            "None"
        ],
        multi=True
    )

    intake["databricks_use_case"] = ask(
        "For Databricks jobs specifically, what is the primary use case?",
        options=[
            "Large-scale data extracts to Delta / S3",
            "Complex transformations / business logic",
            "ML feature engineering",
            "Reporting / BI data prep",
            "Mix of the above",
            "Not using Databricks"
        ]
    )

    # ----------------------------------------------------------------
    section("4 of 6 — AWS & Orchestration")
    # ----------------------------------------------------------------

    intake["aws_account_structure"] = ask(
        "What is your AWS account structure?",
        options=[
            "Single account (dev/stage/prod in same account)",
            "Separate accounts per environment (dev / stage / prod)",
            "Multi-account landing zone (Control Tower / Organizations)",
            "Not decided yet"
        ]
    )

    intake["aws_region"] = ask_free(
        "Primary AWS region? (e.g. us-east-1)",
        default="us-east-1"
    )

    intake["databricks_cloud"] = ask(
        "Databricks is hosted on:",
        options=["AWS", "Azure", "GCP", "Not using Databricks"]
    )

    intake["current_scheduler"] = ask(
        "What schedules PowerCenter workflows today?",
        options=[
            "Informatica built-in scheduler",
            "Control-M",
            "Autosys / CA Workload",
            "cron / shell scripts",
            "UC4 / Automic",
            "Other"
        ]
    )

    intake["target_orchestrator"] = ask(
        "Preferred TARGET orchestration tool?",
        options=[
            "AWS Step Functions",
            "Amazon MWAA (Managed Airflow)",
            "Databricks Workflows",
            "EventBridge Scheduler (simple cron jobs)",
            "Mix — Step Functions + Databricks Workflows",
            "Not decided yet"
        ]
    )

    intake["iac_tool"] = ask(
        "Preferred Infrastructure-as-Code tool?",
        options=["Terraform", "AWS CDK", "CloudFormation", "Pulumi", "Not decided / none yet"]
    )

    # ----------------------------------------------------------------
    section("5 of 6 — Team & CI/CD")
    # ----------------------------------------------------------------

    intake["team_size"] = ask(
        "How many engineers will do migration work?",
        options=["1 – 2", "3 – 5", "6 – 10", "10+"]
    )

    intake["team_skills"] = ask(
        "Primary language skills of the team? (pick all that apply)",
        options=[
            "Python (general)",
            "PySpark",
            "Scala / Spark",
            "SQL (strong)",
            "Java",
            "Shell / Bash"
        ],
        multi=True
    )

    intake["cicd_platform"] = ask(
        "Existing CI/CD platform?",
        options=[
            "GitHub Actions",
            "GitLab CI",
            "Jenkins",
            "AWS CodePipeline",
            "Azure DevOps",
            "None yet"
        ]
    )

    intake["git_platform"] = ask(
        "Git hosting platform?",
        options=["GitHub", "GitLab", "Bitbucket", "AWS CodeCommit", "Azure Repos", "Other"]
    )

    # ----------------------------------------------------------------
    section("6 of 6 — Migration Strategy & Validation")
    # ----------------------------------------------------------------

    intake["migration_approach"] = ask(
        "Migration approach?",
        options=[
            "Lift-and-shift — replicate existing behavior exactly",
            "Modernize-in-flight — refactor patterns while migrating",
            "Hybrid — lift-and-shift first, optimize later"
        ]
    )

    intake["timeline_pressure"] = ask(
        "Timeline pressure?",
        options=[
            "Hard deadline (license expiry / platform decommission)",
            "Soft deadline (business preference)",
            "No hard deadline — quality over speed"
        ]
    )

    intake["hard_deadline"] = ask_free(
        "If hard deadline, when? (e.g. Q3 2025, Dec 2025, or 'none')",
        default="none"
    )

    intake["parallel_run_tolerance"] = ask(
        "How long can you run old and new jobs in parallel for validation?",
        options=[
            "Days only (tight budget / capacity)",
            "1 – 2 weeks",
            "1 month",
            "2 – 3 months",
            "No constraint"
        ]
    )

    intake["validation_bar"] = ask(
        "What constitutes a PASSED migration for a job? (pick all that apply)",
        options=[
            "Row counts match source vs target",
            "Schema / column definitions match",
            "Data hash / checksum on key columns",
            "Null rate and value distribution checks",
            "Business rule assertions (custom SQL checks)",
            "Sign-off from data owner",
            "Downstream consumer sign-off"
        ],
        multi=True
    )

    intake["pii_sensitivity"] = ask(
        "Does any data in scope contain PII or regulated data?",
        options=[
            "Yes — PII (names, emails, SSN, etc.)",
            "Yes — financial / PCI data",
            "Yes — health / HIPAA data",
            "Yes — multiple categories",
            "No sensitive data",
            "Not sure"
        ]
    )

    intake["compliance_requirements"] = ask(
        "Any compliance frameworks that constrain the migration?",
        options=[
            "SOX",
            "HIPAA",
            "GDPR / CCPA",
            "PCI-DSS",
            "None",
            "Multiple — will specify"
        ],
        multi=True
    )

    intake["additional_notes"] = ask_free(
        "Anything else important we should know? (press Enter to skip)"
    )

    # ----------------------------------------------------------------
    # Write output
    # ----------------------------------------------------------------
    out_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "migration-intake.json")
    with open(out_path, "w") as f:
        json.dump(intake, f, indent=2)

    print()
    print("=" * 60)
    print(f"  Done! Saved to: {out_path}")
    print()
    print("  Next step: paste the contents of migration-intake.json")
    print("  back into your Claude Code session.")
    print("=" * 60)
    print()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\n  Interrupted. Run again to restart.")
        sys.exit(1)
