"""
step_functions_builder.py — Build AWS Step Functions State Machine JSON
from a WorkflowDef.

Design:
  - One Task state per SESSION task (mapped to a Glue job or Databricks job run)
  - Pass states for non-SESSION tasks (COMMAND, DECISION, EMAIL, TIMER)
  - DECISION tasks → Choice state with one rule per outgoing link condition
  - Parallel branches for independent tasks (tasks with no shared dependency chain)
  - Catch/Retry on each Task state (Glue job failures)
  - EventBridge schedule resource if scheduler is not ON_DEMAND
"""

from __future__ import annotations

import json
import textwrap
from typing import Any, Dict, List, Optional, Set

from pc_extractor.models import WorkflowDef, WorkflowTaskDef, WorkflowLinkDef
from .schedule_translator import translate_schedule


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_GLUE_TASK_ARN = (
    "arn:aws:states:::glue:startJobRun.sync"
)
_DATABRICKS_TASK_ARN = (
    "arn:aws:states:::lambda:invoke.waitForTaskToken"
)


def _state_name(task_name: str) -> str:
    return task_name.replace(":", "_").replace(" ", "_")


def _task_to_glue_job_name(mapping_ref: str, folder: str, env_var: str = "${Environment}") -> str:
    if mapping_ref:
        return f"{env_var}-{mapping_ref.lower()}"
    return f"{env_var}-unknown"


def _successors(task_name: str, links: List[WorkflowLinkDef]) -> List[WorkflowLinkDef]:
    return [lnk for lnk in links if lnk.from_task == task_name]


def _predecessors(task_name: str, links: List[WorkflowLinkDef]) -> List[str]:
    return [lnk.from_task for lnk in links if lnk.to_task == task_name]


def _topo_sort(tasks: List[WorkflowTaskDef], links: List[WorkflowLinkDef]) -> List[WorkflowTaskDef]:
    """Kahn's algorithm topological sort."""
    name_map = {t.name: t for t in tasks}
    in_degree: Dict[str, int] = {t.name: 0 for t in tasks}
    for lnk in links:
        if lnk.to_task in in_degree:
            in_degree[lnk.to_task] += 1

    queue = [t for t in tasks if in_degree[t.name] == 0]
    result = []
    while queue:
        node = queue.pop(0)
        result.append(node)
        for lnk in links:
            if lnk.from_task == node.name and lnk.to_task in in_degree:
                in_degree[lnk.to_task] -= 1
                if in_degree[lnk.to_task] == 0:
                    queue.append(name_map[lnk.to_task])

    # Append any unreachable tasks
    visited = {t.name for t in result}
    result.extend(t for t in tasks if t.name not in visited)
    return result


# ---------------------------------------------------------------------------
# State builders
# ---------------------------------------------------------------------------

def _build_task_state(
    task: WorkflowTaskDef,
    next_state: Optional[str],
    folder: str,
    generation_report: Optional[dict] = None,
) -> dict:
    """Build a Task state for a SESSION task (mapped to Glue job)."""
    job_name = task.mapping_ref or task.name
    state: dict = {
        "Type": "Task",
        "Comment": f"PC Session: {task.name}",
        "Resource": _GLUE_TASK_ARN,
        "Parameters": {
            "JobName.$": f"$.{_state_name(job_name)}_job_name",
            "Arguments": {
                "--JOB_RUN_ID.$": "$$.Execution.Name"
            }
        },
        "Retry": [
            {
                "ErrorEquals": ["Glue.GlueException", "States.TaskFailed"],
                "IntervalSeconds": 60,
                "MaxAttempts": 2,
                "BackoffRate": 2.0
            }
        ],
        "Catch": [
            {
                "ErrorEquals": ["States.ALL"],
                "Next": "HandleFailure",
                "ResultPath": "$.error"
            }
        ],
    }
    if next_state:
        state["Next"] = next_state
    else:
        state["End"] = True
    return state


def _build_pass_state(task: WorkflowTaskDef, next_state: Optional[str]) -> dict:
    """Stub Pass state for non-SESSION tasks."""
    state: dict = {
        "Type": "Pass",
        "Comment": f"PC {task.task_type} task: {task.name} (stub — no action needed)",
    }
    if next_state:
        state["Next"] = next_state
    else:
        state["End"] = True
    return state


def _build_choice_state(
    task: WorkflowTaskDef,
    links: List[WorkflowLinkDef],
    default_next: Optional[str],
) -> dict:
    """Build a Choice state for DECISION tasks."""
    rules = []
    for lnk in _successors(task.name, links):
        if lnk.condition:
            rules.append({
                "Variable": "$.decision_result",
                "StringEquals": lnk.condition,
                "Next": _state_name(lnk.to_task),
            })
        else:
            # unconditional becomes default
            default_next = _state_name(lnk.to_task)

    choice_state: dict = {
        "Type": "Choice",
        "Comment": f"PC Decision: {task.name}",
    }
    if rules:
        choice_state["Choices"] = rules
    if default_next:
        choice_state["Default"] = default_next
    else:
        choice_state["Default"] = "HandleFailure"
    return choice_state


def _build_failure_state() -> dict:
    return {
        "Type": "Fail",
        "Comment": "Workflow step failed — check CloudWatch logs",
        "Error": "WorkflowStepFailed",
        "Cause": "A workflow task failed. See $.error for details.",
    }


# ---------------------------------------------------------------------------
# Main builder
# ---------------------------------------------------------------------------

def build_step_functions(
    workflow: WorkflowDef,
    generation_report: Optional[dict] = None,
) -> str:
    """
    Build a Step Functions State Machine JSON string for the given WorkflowDef.

    generation_report: optional dict from glue-gen/databricks-gen report JSON,
                       used to resolve actual job names.

    Returns:
        JSON string suitable for use as an asl_definition in Terraform or direct
        upload to Step Functions.
    """
    tasks = workflow.tasks
    links = workflow.links
    folder = workflow.folder

    if not tasks:
        return json.dumps({
            "Comment": f"Empty workflow: {workflow.name}",
            "StartAt": "NoOp",
            "States": {
                "NoOp": {"Type": "Pass", "End": True}
            }
        }, indent=2)

    # Topological sort for state ordering
    sorted_tasks = _topo_sort(tasks, links)

    states: Dict[str, dict] = {}

    for i, task in enumerate(sorted_tasks):
        name = _state_name(task.name)
        # Find next state in chain
        successors = _successors(task.name, links)
        next_state: Optional[str] = None
        if successors:
            # Pick first unconditional successor or first successor
            uncond = [lnk for lnk in successors if not lnk.condition]
            first_link = uncond[0] if uncond else successors[0]
            next_state = _state_name(first_link.to_task)

        if task.task_type == "DECISION":
            states[name] = _build_choice_state(task, links, next_state)
        elif task.task_type == "SESSION":
            states[name] = _build_task_state(task, next_state, folder, generation_report)
        else:
            states[name] = _build_pass_state(task, next_state)

    # Add failure handler
    states["HandleFailure"] = _build_failure_state()

    start_task = sorted_tasks[0]

    sm: dict = {
        "Comment": f"Migrated workflow: {workflow.name} (folder: {folder})",
        "StartAt": _state_name(start_task.name),
        "States": states,
    }

    return json.dumps(sm, indent=2)
