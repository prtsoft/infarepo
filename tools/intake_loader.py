"""
intake_loader.py — Load migration-intake.json from standard locations.

Searches in order:
  1. Explicit path passed to load_intake(path=...)
  2. CWD / migration-intake.json
  3. Repo root (parent dirs up to 3 levels)

The intake file drives environment-specific defaults across all toolchain commands.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_FILENAME = "migration-intake.json"


def load_intake(path: Optional[Path] = None) -> dict:
    """
    Load and return the migration-intake.json config.

    Args:
        path: Explicit file path. If None, searches standard locations.

    Returns:
        Parsed JSON dict. Empty dict if no file is found.
    """
    if path is not None:
        p = Path(path)
        if not p.exists():
            log.warning("intake_loader: specified path does not exist: %s", p)
            return {}
        return _read(p)

    # Auto-discover: CWD and up to 3 parent levels
    search_dirs = [Path.cwd()]
    parent = Path.cwd().parent
    for _ in range(3):
        search_dirs.append(parent)
        parent = parent.parent

    for d in search_dirs:
        candidate = d / _FILENAME
        if candidate.exists():
            log.debug("intake_loader: found at %s", candidate)
            return _read(candidate)

    log.debug("intake_loader: %s not found in search path", _FILENAME)
    return {}


def _read(path: Path) -> dict:
    try:
        with open(path, encoding="utf-8") as fh:
            data = json.load(fh)
        log.info("intake_loader: loaded %s", path)
        return data
    except Exception as exc:
        log.warning("intake_loader: failed to parse %s: %s", path, exc)
        return {}


def get_aws_region(intake: dict, default: str = "us-east-1") -> str:
    return intake.get("aws_region", default)


def get_aws_accounts(intake: dict) -> dict:
    """Returns { "dev": {account_id, region, ...}, "staging": {...}, "prod": {...} }"""
    return intake.get("aws_accounts", {})


def get_terraform_state(intake: dict) -> dict:
    """Returns { "s3_bucket": ..., "dynamodb_table": ... }"""
    return intake.get("terraform_state", {})


def get_orchestrator(intake: dict) -> str:
    """Returns 'step-functions' | 'glue-workflow' | 'airflow' | 'stub'"""
    return intake.get("orchestrator", "stub")


def get_source_databases(intake: dict) -> list:
    return intake.get("source_databases", [])


def get_target_lakehouse(intake: dict) -> dict:
    return intake.get("target_lakehouse", {})


def get_compliance_requirements(intake: dict) -> list:
    return intake.get("compliance_requirements", [])


def is_hipaa(intake: dict) -> bool:
    return "HIPAA" in [c.upper() for c in get_compliance_requirements(intake)]
