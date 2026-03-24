"""
config_gen.py — Auto-generate validation config from field-level lineage.

Generates a YAML validation config with:
  - One validations entry per unique source→target table pair from lineage
  - recon.tolerance_pct: 0.5 default
  - schema_diff.enabled: true
  - rules: null_check for ID/KEY fields, distribution_check for numeric fields
"""

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

_ID_KEY_PATTERN = re.compile(r"(?:_ID|_KEY|_PK|_CODE|SURROGATE_KEY)$", re.IGNORECASE)
_NUMERIC_TYPES  = {"int", "integer", "bigint", "smallint", "decimal", "numeric",
                   "float", "double", "real", "money", "smallmoney", "number"}


def _is_numeric_type(field_type: str) -> bool:
    return (field_type or "").lower().split("(")[0].strip() in _NUMERIC_TYPES


def generate_validation_config(
    lineage,  # MappingLineage from pc_extractor.lineage
    source_dsn: str,
    target_dsn: str,
    tolerance_pct: float = 0.5,
) -> dict:
    """
    Generate a validation config dict from a MappingLineage.

    Args:
        lineage:       MappingLineage result from trace_mapping().
        source_dsn:    Source database connection string.
        target_dsn:    Target lakehouse connection string.
        tolerance_pct: Row count tolerance % (default 0.5%).

    Returns:
        Dict matching the validation YAML schema.
    """
    # Collect unique (source_table, target_table) pairs
    table_pairs: Dict[Tuple[str, str], list] = {}
    for entry in getattr(lineage, "fields", []):
        if entry.source_table and entry.target_table:
            key = (entry.source_table, entry.target_table)
            if key not in table_pairs:
                table_pairs[key] = []
            table_pairs[key].append(entry)

    validations = []
    for (src_table, tgt_table), fields in table_pairs.items():
        rules = []

        # null_check for ID/KEY fields
        id_fields: Set[str] = set()
        for f in fields:
            if f.target_field and _ID_KEY_PATTERN.search(f.target_field):
                if f.target_field not in id_fields:
                    id_fields.add(f.target_field)
                    rules.append({
                        "name": f"no_null_{f.target_field.lower()}",
                        "type": "null_check",
                        "column": f.target_field,
                        "expect": "not_null",
                    })

        # distribution_check for numeric fields (target side)
        numeric_fields: Set[str] = set()
        for f in fields:
            if (f.target_field
                    and _is_numeric_type(getattr(f, "source_field_type", ""))
                    and f.target_field not in id_fields
                    and f.target_field not in numeric_fields):
                numeric_fields.add(f.target_field)
                rules.append({
                    "name": f"distribution_{f.target_field.lower()}",
                    "type": "distribution_check",
                    "column": f.target_field,
                    "buckets": 10,
                    "tolerance_pct": 10.0,
                })

        validations.append({
            "source_table": src_table,
            "target_table": tgt_table,
            "recon": {
                "tolerance_pct": tolerance_pct,
            },
            "schema_diff": {
                "enabled": True,
                "ignore_columns": ["ETL_INSERT_DT", "ETL_UPDATE_DT"],
                "type_equivalences": [
                    ["nvarchar", "string"],
                    ["datetime2", "timestamp"],
                    ["bit", "boolean"],
                    ["decimal", "double"],
                ],
            },
            "rules": rules,
        })

    return {
        "version": "1",
        "connections": {
            "source": {"dsn": source_dsn},
            "target": {"dsn": target_dsn},
        },
        "validations": validations,
    }


def write_validation_config(config: dict, path: Path) -> None:
    """Write a validation config dict to a YAML file."""
    try:
        import yaml
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as fh:
            yaml.dump(config, fh, default_flow_style=False, sort_keys=False)
    except ImportError:
        # Fallback: write JSON if PyYAML not available
        import json
        json_path = path.with_suffix(".json")
        json_path.parent.mkdir(parents=True, exist_ok=True)
        with open(json_path, "w", encoding="utf-8") as fh:
            json.dump(config, fh, indent=2)
