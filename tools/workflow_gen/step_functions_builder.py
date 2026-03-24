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
    if task.task_type.upper() == "COMMAND":
        # COMMAND tasks ran pre/post-session scripts in PC.
        # Migrate to a Lambda invoke, ECS RunTask, or SSM RunCommand Task state.
        script_snippet = (f": {task.command_script[:120]}" if task.command_script else "")
        var_note = (f" Variables: {', '.join(task.task_variables)}." if task.task_variables else "")
        comment = (
            f"TODO: PC COMMAND task '{task.name}'{script_snippet}. "
            f"Replace this Pass state with a Lambda/ECS/SSM Task state.{var_note}"
        )
    else:
        comment = f"PC {task.task_type} task: {task.name} (stub — review manually)"

    state: dict = {
        "Type": "Pass",
        "Comment": comment,
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
    folder = workflow.folder

    # Filter to enabled tasks only; skip disabled PC tasks.
    enabled_tasks = [t for t in workflow.tasks if t.is_enabled]
    enabled_names: Set[str] = {t.name for t in enabled_tasks}
    # Filter links to those between enabled tasks only.
    links = [
        lnk for lnk in workflow.links
        if lnk.from_task in enabled_names and lnk.to_task in enabled_names
    ]

    if not enabled_tasks:
        return json.dumps({
            "Comment": f"Empty workflow (all tasks disabled): {workflow.name}",
            "StartAt": "NoOp",
            "States": {
                "NoOp": {"Type": "Pass", "End": True}
            }
        }, indent=2)

    # Topological sort for state ordering
    sorted_tasks = _topo_sort(enabled_tasks, links)

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

    # Determine start state. If multiple tasks have no predecessors (parallel
    # entry points), wrap them in a Parallel state so none are unreachable.
    entry_tasks = [t for t in sorted_tasks if not _predecessors(t.name, links)]

    if len(entry_tasks) > 1:
        # Build one Branch per entry task. Each branch is a minimal state machine
        # starting at that task and running until it has no further successors.
        # Fan-in convergence is left for manual wiring (complex to detect statically).
        branches = []
        for et in entry_tasks:
            branch_states: Dict[str, dict] = {}
            # Walk this entry task's chain until a task with predecessors from
            # outside this chain is reached (i.e., convergence point).
            visited: Set[str] = set()
            queue = [et.name]
            while queue:
                tname = queue.pop(0)
                if tname in visited:
                    continue
                visited.add(tname)
                # Deep-copy the state so we can mutate Next/End without affecting
                # the original states dict (which is also written to the top-level).
                import copy as _copy
                branch_states[_state_name(tname)] = _copy.deepcopy(states[_state_name(tname)])
                for lnk in links:
                    if lnk.from_task == tname and lnk.to_task not in visited:
                        # Only follow if this successor has no predecessors outside our branch
                        other_preds = [
                            p for p in _predecessors(lnk.to_task, links)
                            if p not in visited
                        ]
                        if not other_preds:
                            queue.append(lnk.to_task)

            # Fix terminal states: any state whose `Next` points outside this branch
            # must become an End state to produce valid ASL JSON.
            branch_state_names = set(branch_states.keys())
            for st_name, st_body in branch_states.items():
                if "Next" in st_body and st_body["Next"] not in branch_state_names:
                    st_body.pop("Next")
                    st_body["End"] = True
                # Choice states use Default / Choices[].Next — fix those too
                if st_body.get("Type") == "Choice":
                    if "Default" in st_body and st_body["Default"] not in branch_state_names:
                        st_body["Default"] = "HandleFailure"
                    if "Choices" in st_body:
                        st_body["Choices"] = [
                            c for c in st_body["Choices"]
                            if c.get("Next") in branch_state_names
                        ]

            branch_start = _state_name(et.name)
            branches.append({"StartAt": branch_start, "States": branch_states})

        states["__ParallelEntryPoints"] = {
            "Type": "Parallel",
            "Comment": (
                "TODO: PC workflow has multiple independent entry tasks — "
                "verify branch boundaries and add convergence logic after this state."
            ),
            "Branches": branches,
            "End": True,
        }
        start_at = "__ParallelEntryPoints"
    else:
        start_at = _state_name(sorted_tasks[0].name)

    sm: dict = {
        "Comment": f"Migrated workflow: {workflow.name} (folder: {folder})",
        "StartAt": start_at,
        "States": states,
    }

    return json.dumps(sm, indent=2)
