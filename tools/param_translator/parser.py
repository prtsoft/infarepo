"""
PowerCenter .prm file parser.

.prm file format:
  [folder_name.workflow_name:session_task_name]   ← SESSION section
  [folder_name.workflow_name]                      ← WORKFLOW section
  [Global]                                         ← GLOBAL section
  $$PARAM_NAME=value                               ← parameter
  # comment line                                   ← ignored
  ; comment line                                   ← ignored

Multi-line values:
  PowerCenter technically writes one value per line, but in practice teams
  write long SQL across multiple lines using either:
    a) backslash continuation:  $$SQL=SELECT * \\\n  FROM TABLE
    b) bare continuation:       lines after $$PARAM= that don't start with
                                $$, [, #, or ; are treated as value continuations

Section name variants:
  [Global]                          → GLOBAL
  [FOLDER_NAME.WF_NAME]             → WORKFLOW
  [FOLDER_NAME.WF_NAME:s_MAPPING]   → SESSION
  [FOLDER_NAME.WF_NAME:cmd_notify]  → SESSION (non-mapping tasks still parsed)
  Multiple dots in folder/workflow names are supported.
"""

from __future__ import annotations
import logging
import re
from pathlib import Path
from typing import List, Optional, Tuple

from .models import PrmFile, PrmParameter, PrmSection, SectionType

log = logging.getLogger(__name__)

# Matches [anything] — section header
_SECTION_RE = re.compile(r"^\s*\[(.+?)\]\s*$")

# Matches $$NAME=value or $NAME=value
_PARAM_RE = re.compile(r"^\s*(\$\$?[A-Za-z0-9_]+)\s*=(.*)$")

# Comment prefixes
_COMMENT_PREFIXES = ("#", ";", "//")


# ---------------------------------------------------------------------------
# Section header parser
# ---------------------------------------------------------------------------

def _parse_section_header(header: str) -> PrmSection:
    """
    Parse a raw section header string (without brackets) into a PrmSection.

    Examples:
      "Global"                           → GLOBAL,  folder="", workflow=""
      "SALES_MART.WF_DAILY"             → WORKFLOW, folder="SALES_MART", workflow="WF_DAILY"
      "SALES_MART.WF_DAILY:s_M_LOAD"   → SESSION,  folder="SALES_MART", workflow="WF_DAILY", task="s_M_LOAD"
    """
    stripped = header.strip()

    # Global section
    if stripped.upper() == "GLOBAL":
        return PrmSection(
            raw_header=f"[{header}]",
            folder="",
            workflow="",
            task=None,
            section_type=SectionType.GLOBAL,
        )

    # Split on colon to separate session task
    task: Optional[str] = None
    if ":" in stripped:
        wf_part, task = stripped.rsplit(":", 1)
        task = task.strip()
    else:
        wf_part = stripped

    # Split folder from workflow on last dot
    # Use rsplit so folder names with dots work: FINANCE.DEPT.WF_NAME
    if "." in wf_part:
        folder, workflow = wf_part.rsplit(".", 1)
    else:
        # No dot — treat whole thing as workflow, no folder
        folder = ""
        workflow = wf_part

    section_type = SectionType.SESSION if task else SectionType.WORKFLOW

    return PrmSection(
        raw_header=f"[{header}]",
        folder=folder.strip(),
        workflow=workflow.strip(),
        task=task,
        section_type=section_type,
    )


# ---------------------------------------------------------------------------
# Main parser
# ---------------------------------------------------------------------------

def parse_prm_file(path: Path) -> PrmFile:
    """
    Parse a single .prm file into a PrmFile object.
    Handles multi-line values, comments, and all section types.
    """
    text = path.read_text(encoding="utf-8", errors="replace")
    return parse_prm_text(text, source_path=str(path))


def parse_prm_text(text: str, source_path: str = "<string>") -> PrmFile:
    """
    Parse .prm content from a string.
    Useful for testing and for reading from S3.
    """
    prm = PrmFile(source_path=source_path, raw_text=text)

    # Implicit global section for params before any [header]
    current_section: PrmSection = PrmSection(
        raw_header="[Global]",
        folder="", workflow="", task=None,
        section_type=SectionType.GLOBAL,
    )
    last_param: Optional[PrmParameter] = None

    lines = text.splitlines()

    for lineno, raw_line in enumerate(lines, start=1):
        line = raw_line.rstrip("\r")

        # ---- Empty line ----
        if not line.strip():
            last_param = None   # blank line ends multi-line continuation
            continue

        # ---- Comment ----
        stripped = line.lstrip()
        if any(stripped.startswith(p) for p in _COMMENT_PREFIXES):
            continue

        # ---- Section header ----
        section_match = _SECTION_RE.match(line)
        if section_match:
            # Save current section if it has params (or it's the implicit global)
            if current_section.params or current_section.section_type == SectionType.GLOBAL:
                prm.sections.append(current_section)
            current_section = _parse_section_header(section_match.group(1))
            last_param = None
            continue

        # ---- Parameter line ----
        param_match = _PARAM_RE.match(line)
        if param_match:
            raw_name = param_match.group(1).strip()
            raw_value = param_match.group(2)

            # Strip inline comments (value; comment  or  value # comment)
            # Be careful not to strip # inside SQL strings
            raw_value = _strip_inline_comment(raw_value)

            # Handle backslash continuation on the same line
            if raw_value.endswith("\\"):
                raw_value = raw_value[:-1]

            param = PrmParameter(
                raw_name=raw_name,
                name=raw_name.lstrip("$"),
                raw_value=raw_value,
                param_type="",   # filled by classifier
                source_line=lineno,
            )
            current_section.params[param.name] = param
            last_param = param
            log.debug("  param: %s = %r (line %d)", raw_name, raw_value[:60], lineno)
            continue

        # ---- Multi-line value continuation ----
        if last_param is not None:
            continuation = line.rstrip("\\")
            last_param.raw_value = last_param.raw_value + "\n" + continuation
            log.debug("  continuation for %s (line %d)", last_param.name, lineno)
            continue

        # Unrecognised line — log and skip
        log.debug("Unrecognised line %d in %s: %r", lineno, source_path, line[:80])

    # Append the final section
    if current_section.params or current_section.section_type == SectionType.GLOBAL:
        # Don't add empty implicit global twice
        already_added = any(
            s.section_type == SectionType.GLOBAL and not s.params
            for s in prm.sections
        )
        if not already_added or current_section.params:
            prm.sections.append(current_section)

    # Deduplicate — if the implicit global has no params and a real [Global] exists, drop it
    prm.sections = _deduplicate_globals(prm.sections)

    log.info(
        "Parsed %s: %d section(s), %d param(s) total",
        source_path,
        len(prm.sections),
        sum(len(s.params) for s in prm.sections),
    )
    return prm


def _strip_inline_comment(value: str) -> str:
    """
    Remove trailing inline comments from a value string.
    Avoids stripping # or ; that appear inside single or double quotes.
    """
    in_quote = False
    quote_char = None
    for i, ch in enumerate(value):
        if in_quote:
            if ch == quote_char:
                in_quote = False
        elif ch in ("'", '"'):
            in_quote = True
            quote_char = ch
        elif ch in ("#", ";"):
            # Only strip if preceded by whitespace (genuine comment)
            if i > 0 and value[i - 1] in (" ", "\t"):
                return value[:i].rstrip()
    return value


def _deduplicate_globals(sections: List[PrmSection]) -> List[PrmSection]:
    """
    If there's both an implicit empty Global and an explicit [Global],
    keep only the explicit one.
    """
    explicit_globals = [s for s in sections if s.section_type == SectionType.GLOBAL and s.params]
    implicit_empty   = [s for s in sections if s.section_type == SectionType.GLOBAL and not s.params]
    non_globals      = [s for s in sections if s.section_type != SectionType.GLOBAL]

    if explicit_globals:
        return explicit_globals + non_globals
    if implicit_empty:
        return implicit_empty[:1] + non_globals   # keep at most one empty global
    return non_globals


# ---------------------------------------------------------------------------
# Multi-file parser
# ---------------------------------------------------------------------------

def parse_prm_files(paths: List[Path]) -> List[PrmFile]:
    """Parse multiple .prm files, returning one PrmFile per file."""
    result = []
    for path in paths:
        try:
            result.append(parse_prm_file(path))
        except Exception as exc:
            log.error("Failed to parse %s: %s", path, exc)
    return result
