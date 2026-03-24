"""
Data models for parsed PowerCenter parameter files.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import Dict, List, Optional


class ParamType:
    DATE       = "DATE"         # a concrete date value  e.g. 01/15/2024
    DATE_MASK  = "DATE_MASK"    # a format string        e.g. MM/DD/YYYY
    SQL        = "SQL"          # a SQL fragment/query   e.g. SELECT ... FROM ...
    INTEGER    = "INTEGER"      # whole number           e.g. 100
    DECIMAL    = "DECIMAL"      # decimal number         e.g. 3.14
    BOOLEAN    = "BOOLEAN"      # Y/N/YES/NO/TRUE/FALSE/1/0
    PATH       = "PATH"         # filesystem or UNC path e.g. /data/input/
    STRING     = "STRING"       # everything else
    EMPTY      = "EMPTY"        # explicitly empty value


class SectionType:
    GLOBAL   = "GLOBAL"    # [Global] or top-level params before any section
    WORKFLOW = "WORKFLOW"  # [folder.workflow]
    SESSION  = "SESSION"   # [folder.workflow:session_task]


@dataclass
class PrmParameter:
    raw_name: str              # original name including $$, e.g. $$START_DATE
    name: str                  # stripped name, e.g. START_DATE
    raw_value: str             # exact string from file (may be multi-line joined)
    param_type: str            # one of ParamType.*
    # Normalized / translated values
    normalized_value: str = ""      # cleaned value (ISO dates, trimmed SQL, etc.)
    spark_value: str = ""           # PySpark-compatible value or expression
    glue_arg_name: str = ""         # --ARG_NAME for Glue getResolvedOptions
    notes: List[str] = field(default_factory=list)
    source_line: int = 0            # line number in source file


@dataclass
class PrmSection:
    raw_header: str            # full [bracket text] including brackets
    folder: str                # folder name
    workflow: str              # workflow name
    task: Optional[str]        # session/task name (None for workflow-level)
    section_type: str          # SectionType.*
    params: Dict[str, PrmParameter] = field(default_factory=dict)

    @property
    def key(self) -> str:
        """Canonical section key: FOLDER.WORKFLOW or FOLDER.WORKFLOW:TASK"""
        base = f"{self.folder}.{self.workflow}"
        return f"{base}:{self.task}" if self.task else base


@dataclass
class PrmFile:
    source_path: str
    raw_text: str
    sections: List[PrmSection] = field(default_factory=list)

    @property
    def merged(self) -> Dict[str, PrmParameter]:
        """
        All params merged in section order.
        Global < Workflow < Session (later/more-specific wins).
        """
        result: Dict[str, PrmParameter] = {}
        # Apply global first, then workflow, then session
        for section in sorted(self.sections, key=lambda s: (
            0 if s.section_type == SectionType.GLOBAL   else
            1 if s.section_type == SectionType.WORKFLOW else 2
        )):
            result.update(section.params)
        return result

    @property
    def all_section_keys(self) -> List[str]:
        return [s.key for s in self.sections]
