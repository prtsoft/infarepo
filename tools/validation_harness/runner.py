"""
Validation runner — orchestrates recon + schema diff + business rules
from a YAML config file.

Config file format (YAML):

  version: "1"

  connections:
    source:
      dsn: "mssql+pyodbc://user:pass@host:1433/MyDB?driver=ODBC+Driver+17+for+SQL+Server"
    target:
      dsn: "databricks://token:${DATABRICKS_TOKEN}@host/default"

  validations:
    - source_table: "dbo.ORDERS"
      target_table: "orders"

      recon:
        tolerance_pct: 1.0          # allow up to 1% row count difference
        source_sql: null            # optional COUNT(*) override for source
        target_sql: null            # optional COUNT(*) override for target

      schema_diff:
        enabled: true
        ignore_columns:             # target-only audit/ETL columns
          - ETL_INSERT_DT
          - ETL_UPDATE_DT
        type_equivalences:
          - [nvarchar, string]
          - [datetime2, timestamp]
          - [bit, boolean]

      rules:
        - name: "no null order IDs"
          type: null_check
          column: ORDER_ID
          expect: not_null

        - name: "order amount non-negative"
          type: range_check
          column: ORDER_AMT
          min: 0

        - name: "unique order IDs"
          type: unique_check
          column: ORDER_ID

        - name: "valid status values"
          type: value_set
          column: STATUS
          allowed_values: [PENDING, APPROVED, CANCELLED, RETURNED]

        - name: "customer FK valid"
          type: referential
          column: CUSTOMER_ID
          parent_table: customers
          parent_column: CUSTOMER_ID

        - name: "no future order dates"
          type: custom_sql
          sql: "SELECT COUNT(*) FROM {table} WHERE ORDER_DATE > CURRENT_DATE"
          expect_count: 0
"""
from __future__ import annotations

import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from .connections import Connection, create_connection
from .models import (
    ReconResult, SchemaDiff, RuleResult,
    ValidationReport, ValidationSummary,
    _redact_dsn,
)
from .recon import run_recon, diff_schemas
from .rules import build_and_evaluate

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Config loading
# ---------------------------------------------------------------------------

def _expand_env(value: str) -> str:
    """Expand ${VAR} environment variable references in a string."""
    return re.sub(
        r"\$\{([^}]+)\}",
        lambda m: os.environ.get(m.group(1), m.group(0)),
        value,
    )


def load_config(config_path: Path) -> dict:
    """Load and parse a YAML validation config file."""
    try:
        import yaml
    except ImportError:
        raise RuntimeError(
            "PyYAML is required to load validation config. "
            "Install it with: pip install pyyaml"
        )
    with open(config_path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)

    # Expand environment variables in DSN strings
    for conn_name, conn_cfg in raw.get("connections", {}).items():
        if "dsn" in conn_cfg:
            conn_cfg["dsn"] = _expand_env(conn_cfg["dsn"])

    return raw


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run_validation(
    config_path: Path,
    *,
    source_conn: Optional[Connection] = None,
    target_conn: Optional[Connection] = None,
    run_recon_only: bool = False,
    run_schema_only: bool = False,
    dry_run: bool = False,
) -> ValidationReport:
    """
    Run the full validation suite from a YAML config file.

    Args:
        config_path:      Path to the YAML config file.
        source_conn:      Override source connection (for testing).
        target_conn:      Override target connection (for testing).
        run_recon_only:   Skip schema diff and rules.
        run_schema_only:  Skip recon and rules.
        dry_run:          Parse config and log plan but don't execute SQL.
    """
    config_path = Path(config_path)
    config = load_config(config_path)

    src_dsn = config.get("connections", {}).get("source", {}).get("dsn", "mock://")
    tgt_dsn = config.get("connections", {}).get("target", {}).get("dsn", "mock://")

    if source_conn is None:
        source_conn = create_connection(src_dsn, mock=dry_run)
    if target_conn is None:
        target_conn = create_connection(tgt_dsn, mock=dry_run)

    recon_results:  List[ReconResult]  = []
    schema_diffs:   List[SchemaDiff]   = []
    rule_results:   List[RuleResult]   = []

    validations = config.get("validations", [])
    log.info("Running %d validation(s) from %s", len(validations), config_path.name)

    for val_cfg in validations:
        src_table = val_cfg["source_table"]
        tgt_table = val_cfg["target_table"]

        # --- Recon ---
        if not run_schema_only:
            recon_cfg = val_cfg.get("recon", {})
            result = run_recon(
                source_conn=source_conn,
                target_conn=target_conn,
                source_table=src_table,
                target_table=tgt_table,
                tolerance_pct=float(recon_cfg.get("tolerance_pct", 1.0)),
                source_sql=recon_cfg.get("source_sql"),
                target_sql=recon_cfg.get("target_sql"),
            )
            recon_results.append(result)

        if run_recon_only:
            continue

        # --- Schema diff ---
        schema_cfg = val_cfg.get("schema_diff", {})
        if schema_cfg.get("enabled", True) and not run_schema_only or run_schema_only:
            diff = diff_schemas(
                source_conn=source_conn,
                target_conn=target_conn,
                source_table=src_table,
                target_table=tgt_table,
                ignored_columns=schema_cfg.get("ignore_columns", []),
                type_equivalences=[
                    tuple(p) for p in schema_cfg.get("type_equivalences", [])
                ],
            )
            schema_diffs.append(diff)

        # --- Business rules ---
        for rule_dict in val_cfg.get("rules", []):
            try:
                result = build_and_evaluate(target_conn, tgt_table, rule_dict)
                rule_results.append(result)
            except Exception as exc:
                log.error("Rule %r failed with error: %s", rule_dict.get("name"), exc)

    # --- Summary ---
    hipaa_flags = sum(1 for r in rule_results if r.hipaa_flagged)
    summary = ValidationSummary(
        files_validated=1,
        recon_total=len(recon_results),
        recon_passed=sum(1 for r in recon_results if r.passed),
        schema_diff_total=len(schema_diffs),
        schema_diff_passed=sum(1 for d in schema_diffs if d.passed),
        rules_total=len(rule_results),
        rules_passed=sum(1 for r in rule_results if r.passed),
        hipaa_flags=hipaa_flags,
        overall_passed=(
            all(r.passed for r in recon_results)
            and all(d.passed for d in schema_diffs)
            and all(r.passed for r in rule_results)
        ),
    )

    return ValidationReport(
        generated=datetime.now(timezone.utc).isoformat(),
        source_dsn=_redact_dsn(src_dsn),
        target_dsn=_redact_dsn(tgt_dsn),
        config_path=str(config_path),
        recon_results=recon_results,
        schema_diffs=schema_diffs,
        rule_results=rule_results,
        summary=summary,
    )
