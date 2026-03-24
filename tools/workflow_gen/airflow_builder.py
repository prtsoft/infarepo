"""
airflow_builder.py — Build Apache Airflow 2.x DAG from a WorkflowDef.

Uses:
  - GlueJobOperator for GLUE-routed mappings
  - DatabricksRunNowOperator for DATABRICKS-routed mappings
  - EmptyOperator for non-session tasks (COMMAND, DECISION, EMAIL, TIMER)
  - Dependency wiring via >> operator from link definitions
"""

from __future__ import annotations

import textwrap
from typing import Dict, List, Optional

from pc_extractor.models import WorkflowDef, WorkflowTaskDef, WorkflowLinkDef
from .schedule_translator import translate_schedule


def _safe_py(name: str) -> str:
    """Python identifier safe name."""
    import re
    return re.sub(r"[^a-z0-9_]", "_", name.lower()).strip("_")


def _dag_id(workflow: WorkflowDef) -> str:
    return _safe_py(f"{workflow.folder}_{workflow.name}")


def build_airflow_dag(
    workflow: WorkflowDef,
    platform_map: Optional[Dict[str, str]] = None,
    default_platform: str = "glue",
) -> str:
    """
    Build an Airflow 2.x DAG Python file for the given WorkflowDef.

    Args:
        workflow:         The workflow to convert.
        platform_map:     Optional dict {mapping_ref -> 'glue'|'databricks'} to choose operator.
        default_platform: Default operator type when not in platform_map.

    Returns:
        Python source code string for the DAG file.
    """
    platform_map = platform_map or {}
    dag_id = _dag_id(workflow)
    cron = translate_schedule(workflow.scheduler)
    if cron.startswith("#"):
        schedule = "None  # " + cron
    else:
        schedule = f'"{cron}"'

    # Collect import needs
    needs_glue = any(
        platform_map.get(t.mapping_ref or t.name, default_platform) == "glue"
        for t in workflow.tasks if t.task_type == "SESSION"
    )
    needs_databricks = any(
        platform_map.get(t.mapping_ref or t.name, default_platform) == "databricks"
        for t in workflow.tasks if t.task_type == "SESSION"
    )

    imports = [
        "from datetime import datetime",
        "from airflow import DAG",
        "from airflow.operators.empty import EmptyOperator",
    ]
    if needs_glue:
        imports.append("from airflow.providers.amazon.aws.operators.glue import GlueJobOperator")
    if needs_databricks:
        imports.append("from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator")

    import_block = "\n".join(imports)

    # DAG header
    header = textwrap.dedent(f"""\
        {import_block}

        # ---------------------------------------------------------------
        # DAG: {dag_id}
        # Workflow: {workflow.name}  |  Folder: {workflow.folder}
        # Migrated from Informatica PowerCenter 10.5
        # ---------------------------------------------------------------

        with DAG(
            dag_id="{dag_id}",
            description="{workflow.description or workflow.name}",
            schedule_interval={schedule},
            start_date=datetime(2024, 1, 1),
            catchup=False,
            tags=["{workflow.folder}", "migrated-from-informatica"],
        ) as dag:
    """)

    task_lines = []
    task_vars: Dict[str, str] = {}

    for task in workflow.tasks:
        safe = _safe_py(task.name)
        task_vars[task.name] = safe

        if task.task_type == "SESSION" and task.mapping_ref:
            platform = platform_map.get(task.mapping_ref, default_platform)
            if platform == "databricks":
                task_lines.append(textwrap.indent(textwrap.dedent(f"""\
                    {safe} = DatabricksRunNowOperator(
                        task_id="{safe}",
                        job_name="{task.mapping_ref}",
                        databricks_conn_id="databricks_default",
                    )
                """), "    "))
            else:
                task_lines.append(textwrap.indent(textwrap.dedent(f"""\
                    {safe} = GlueJobOperator(
                        task_id="{safe}",
                        job_name="${{var.environment}}-{task.mapping_ref.lower()}",
                        aws_conn_id="aws_default",
                        region_name="{{}}" .format("{{ var.aws_region }}"),
                        script_args={{
                            "--JOB_RUN_ID": "{{{{ run_id }}}}",
                        }},
                        wait_for_completion=True,
                    )
                """), "    "))
        else:
            task_lines.append(textwrap.indent(textwrap.dedent(f"""\
                {safe} = EmptyOperator(
                    task_id="{safe}",
                    # PC task type: {task.task_type}
                )
            """), "    "))

    # Dependency wiring from links
    dep_lines = []
    for link in workflow.links:
        from_var = task_vars.get(link.from_task)
        to_var   = task_vars.get(link.to_task)
        if from_var and to_var:
            if link.condition:
                dep_lines.append(f"    # Link condition: {link.condition}")
            dep_lines.append(f"    {from_var} >> {to_var}")

    if not dep_lines:
        # No links: chain all tasks sequentially
        task_list = list(task_vars.values())
        if len(task_list) > 1:
            chain_str = " >> ".join(task_list)
            dep_lines.append(f"    {chain_str}")

    return header + "\n".join(task_lines) + "\n" + "\n".join(dep_lines) + "\n"


def build_airflow_dag_stub(workflow: WorkflowDef) -> str:
    """Minimal stub DAG for unsupported task types."""
    dag_id = _dag_id(workflow)
    return textwrap.dedent(f"""\
        from airflow import DAG
        from airflow.operators.empty import EmptyOperator
        from datetime import datetime

        # TODO: Implement DAG for workflow: {workflow.name}
        with DAG(dag_id="{dag_id}", start_date=datetime(2024, 1, 1), catchup=False) as dag:
            start = EmptyOperator(task_id="start")
            end   = EmptyOperator(task_id="end")
            start >> end
    """)
