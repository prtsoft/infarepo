"""
Data models for validation-harness results and configuration.

HIPAA safety contract:
  - No actual data values appear in any model field.
  - Only counts, percentages, table names, column names, and pass/fail status.
  - Credentials are redacted from DSN strings before storage.
"""
from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Connection config
# ---------------------------------------------------------------------------

def _redact_dsn(dsn: str) -> str:
    """Replace password in DSN connection string with ***."""
    # SQLAlchemy DSN: scheme://user:password@host/db
    redacted = re.sub(r"(://[^:@/]+:)[^@]+(@)", r"\1***\2", dsn)
    # ODBC-style: PWD=secret;
    redacted = re.sub(r"(?i)(PWD\s*=\s*)[^;]+", r"\1***", redacted)
    # Token-style: token:secret@
    redacted = re.sub(r"(token:)[^@]+(@)", r"\1***\2", redacted)
    return redacted


@dataclass
class ConnectionConfig:
    dsn: str
    name: str = "connection"

    @property
    def redacted_dsn(self) -> str:
        return _redact_dsn(self.dsn)

    @property
    def dialect(self) -> str:
        """Return the DB dialect: sqlserver, databricks, athena, sqlite, etc."""
        lower = self.dsn.lower()
        if "mssql" in lower or "sqlserver" in lower:
            return "sqlserver"
        if "databricks" in lower:
            return "databricks"
        if "athena" in lower:
            return "athena"
        if "oracle" in lower:
            return "oracle"
        if "sqlite" in lower:
            return "sqlite"
        if "postgresql" in lower or "postgres" in lower:
            return "postgresql"
        return "unknown"


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

@dataclass
class ReconResult:
    table_pair: str          # "dbo.ORDERS → target.orders"
    source_table: str
    target_table: str
    source_count: int
    target_count: int
    delta: int               # target_count - source_count
    delta_pct: float         # abs(delta) / source_count * 100
    tolerance_pct: float
    passed: bool
    notes: List[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Schema diff
# ---------------------------------------------------------------------------

@dataclass
class SchemaField:
    name: str
    dtype: str               # normalized: string, integer, decimal, date, timestamp, boolean, binary
    nullable: bool = True
    precision: Optional[int] = None
    scale: Optional[int] = None


@dataclass
class TypeMismatch:
    column: str
    source_type: str
    target_type: str
    compatible: bool         # True if types are semantically equivalent


@dataclass
class SchemaDiff:
    table_pair: str
    source_table: str
    target_table: str
    source_columns: List[SchemaField]
    target_columns: List[SchemaField]
    missing_in_target: List[str]    # columns in source not in target
    extra_in_target: List[str]      # columns in target not in source (e.g. audit cols)
    type_mismatches: List[TypeMismatch]
    ignored_columns: List[str]
    passed: bool


# ---------------------------------------------------------------------------
# Business rules
# ---------------------------------------------------------------------------

_HIPAA_COLUMN_RE = re.compile(
    r"(?:^|_)(PHI|PII|SSN|DOB|MRN|DOD|PATIENT|NAME|ADDRESS|PHONE|EMAIL|"
    r"BIRTH|GENDER|RACE|ETHNICITY|DIAGNOSIS|PROCEDURE|NPI|INSURANCE)(?:_|$)",
    re.IGNORECASE,
)


@dataclass
class RuleResult:
    name: str
    rule_type: str           # null_check, range_check, unique_check, referential, custom_sql
    table: str
    column: Optional[str]
    passed: bool
    failing_count: int       # rows that fail the rule — count only (HIPAA-safe)
    total_count: int
    fail_pct: float
    message: str
    hipaa_flagged: bool = False   # True when column name suggests PII/PHI

    @classmethod
    def make(
        cls,
        name: str,
        rule_type: str,
        table: str,
        column: Optional[str],
        failing_count: int,
        total_count: int,
        threshold_count: int = 0,
    ) -> "RuleResult":
        passed = failing_count <= threshold_count
        fail_pct = (failing_count / total_count * 100) if total_count else 0.0
        hipaa = bool(column and _HIPAA_COLUMN_RE.search(column))
        msg_parts = [f"{failing_count}/{total_count} rows failing ({fail_pct:.2f}%)"]
        if hipaa:
            msg_parts.append("HIPAA: column may contain PII — values not logged")
        return cls(
            name=name,
            rule_type=rule_type,
            table=table,
            column=column,
            passed=passed,
            failing_count=failing_count,
            total_count=total_count,
            fail_pct=fail_pct,
            message="; ".join(msg_parts),
            hipaa_flagged=hipaa,
        )


# ---------------------------------------------------------------------------
# Validation report
# ---------------------------------------------------------------------------

@dataclass
class ValidationSummary:
    files_validated: int
    recon_total: int
    recon_passed: int
    schema_diff_total: int
    schema_diff_passed: int
    rules_total: int
    rules_passed: int
    hipaa_flags: int
    overall_passed: bool

    def to_dict(self) -> Dict[str, Any]:
        return {
            "files_validated":     self.files_validated,
            "recon_total":         self.recon_total,
            "recon_passed":        self.recon_passed,
            "recon_failed":        self.recon_total - self.recon_passed,
            "schema_diff_total":   self.schema_diff_total,
            "schema_diff_passed":  self.schema_diff_passed,
            "schema_diff_failed":  self.schema_diff_total - self.schema_diff_passed,
            "rules_total":         self.rules_total,
            "rules_passed":        self.rules_passed,
            "rules_failed":        self.rules_total - self.rules_passed,
            "hipaa_flags":         self.hipaa_flags,
            "overall_passed":      self.overall_passed,
        }


@dataclass
class ValidationReport:
    generated: str
    source_dsn: str          # credentials redacted
    target_dsn: str          # credentials redacted
    config_path: str
    recon_results: List[ReconResult]
    schema_diffs: List[SchemaDiff]
    rule_results: List[RuleResult]
    summary: ValidationSummary

    def to_dict(self) -> Dict[str, Any]:
        return {
            "_generated":   self.generated,
            "source_dsn":   self.source_dsn,
            "target_dsn":   self.target_dsn,
            "config_path":  self.config_path,
            "summary":      self.summary.to_dict(),
            "recon": [
                {
                    "table_pair":    r.table_pair,
                    "source_table":  r.source_table,
                    "target_table":  r.target_table,
                    "source_count":  r.source_count,
                    "target_count":  r.target_count,
                    "delta":         r.delta,
                    "delta_pct":     round(r.delta_pct, 4),
                    "tolerance_pct": r.tolerance_pct,
                    "passed":        r.passed,
                    "notes":         r.notes,
                }
                for r in self.recon_results
            ],
            "schema_diffs": [
                {
                    "table_pair":        d.table_pair,
                    "source_table":      d.source_table,
                    "target_table":      d.target_table,
                    "missing_in_target": d.missing_in_target,
                    "extra_in_target":   d.extra_in_target,
                    "type_mismatches": [
                        {
                            "column":      m.column,
                            "source_type": m.source_type,
                            "target_type": m.target_type,
                            "compatible":  m.compatible,
                        }
                        for m in d.type_mismatches
                    ],
                    "ignored_columns": d.ignored_columns,
                    "passed":          d.passed,
                }
                for d in self.schema_diffs
            ],
            "rules": [
                {
                    "name":          r.name,
                    "rule_type":     r.rule_type,
                    "table":         r.table,
                    "column":        r.column,
                    "passed":        r.passed,
                    "failing_count": r.failing_count,
                    "total_count":   r.total_count,
                    "fail_pct":      round(r.fail_pct, 4),
                    "message":       r.message,
                    "hipaa_flagged": r.hipaa_flagged,
                }
                for r in self.rule_results
            ],
        }
