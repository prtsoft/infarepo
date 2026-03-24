"""
Output data models for databricks-gen.
"""
from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional


@dataclass
class NotebookCell:
    cell_type: str   # "markdown" or "code"
    source: str


@dataclass
class DatabricksNotebook:
    mapping_name: str
    folder: str
    cells: List[NotebookCell] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


@dataclass
class NotebookGenerationResult:
    mapping_name: str
    folder: str
    notebook_path: Optional[str] = None
    tf_path: Optional[str] = None
    warnings: List[str] = field(default_factory=list)
    skipped: bool = False
    skip_reason: str = ""


@dataclass
class GenerationReport:
    total: int = 0
    generated: int = 0
    skipped: int = 0
    warnings_count: int = 0
    results: List[NotebookGenerationResult] = field(default_factory=list)
