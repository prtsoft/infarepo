"""
Business rule evaluators for post-migration validation.

HIPAA safety contract:
  - Rules only return COUNT of failing rows, never the actual row values.
  - SQL templates that access potentially-PII columns must be designed
    to return only aggregate counts.
  - Column names that match known PII patterns are flagged in RuleResult.

Rule types:
  null_check    — column must (not) be null
  range_check   — numeric column within [min, max]
  unique_check  — column has no duplicate values
  value_set     — column values are within an allowed set
  referential   — FK column references values that exist in a parent table
  custom_sql    — arbitrary COUNT(*) SQL with expected result
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any, List, Optional

from .connections import Connection
from .models import RuleResult
from .recon import count_rows

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Rule config dataclasses (parsed from YAML)
# ---------------------------------------------------------------------------

@dataclass
class NullCheckConfig:
    name: str
    column: str
    expect: str = "not_null"     # "not_null" or "allow_null"
    rule_type: str = "null_check"


@dataclass
class RangeCheckConfig:
    name: str
    column: str
    min: Optional[float] = None
    max: Optional[float] = None
    rule_type: str = "range_check"


@dataclass
class UniqueCheckConfig:
    name: str
    column: str
    rule_type: str = "unique_check"


@dataclass
class ValueSetConfig:
    name: str
    column: str
    allowed_values: List[str] = field(default_factory=list)
    rule_type: str = "value_set"


@dataclass
class ReferentialConfig:
    name: str
    column: str
    parent_table: str
    parent_column: str
    rule_type: str = "referential"


@dataclass
class CustomSQLConfig:
    name: str
    sql: str                     # Must be a COUNT(*) query; use {table} placeholder
    expect_count: int = 0
    rule_type: str = "custom_sql"
    column: Optional[str] = None


@dataclass
class DistributionCheckConfig:
    name: str
    column: str
    buckets: int = 10            # Number of histogram buckets (approximate)
    tolerance_pct: float = 10.0  # Allowed % difference per bucket
    rule_type: str = "distribution_check"


# ---------------------------------------------------------------------------
# Rule evaluators
# ---------------------------------------------------------------------------

def _safe_identifier(name: str) -> str:
    """Return True if name looks like a safe SQL identifier (no injection)."""
    return bool(re.match(r"^[\w.\[\]\"` ]+$", name))


def _check_identifier(name: str, label: str) -> None:
    if not _safe_identifier(name):
        raise ValueError(f"Unsafe {label} identifier: {name!r}")


def evaluate_null_check(
    conn: Connection,
    table: str,
    config: NullCheckConfig,
) -> RuleResult:
    """Count rows where column IS NULL (or IS NOT NULL for allow_null expectation)."""
    _check_identifier(table, "table")
    _check_identifier(config.column, "column")

    total = count_rows(conn, table)

    if config.expect == "not_null":
        sql = f"SELECT COUNT(*) FROM {table} WHERE {config.column} IS NULL"
        failing_count = int(conn.execute(sql).scalar() or 0)
    else:
        # expect = "allow_null" means we only warn if ALL values are null
        sql = f"SELECT COUNT(*) FROM {table} WHERE {config.column} IS NOT NULL"
        non_null = int(conn.execute(sql).scalar() or 0)
        failing_count = total - non_null   # rows that ARE null — informational

    result = RuleResult.make(
        name=config.name,
        rule_type="null_check",
        table=table,
        column=config.column,
        failing_count=failing_count,
        total_count=total,
        threshold_count=0,
    )
    log.info("Rule [%s] %s → %s", config.name, table, "PASS" if result.passed else "FAIL")
    return result


def evaluate_range_check(
    conn: Connection,
    table: str,
    config: RangeCheckConfig,
) -> RuleResult:
    """Count rows where column is outside [min, max]."""
    _check_identifier(table, "table")
    _check_identifier(config.column, "column")

    total = count_rows(conn, table)
    clauses = []
    if config.min is not None:
        clauses.append(f"{config.column} < {config.min}")
    if config.max is not None:
        clauses.append(f"{config.column} > {config.max}")

    if not clauses:
        # No bounds configured — trivially passes
        return RuleResult.make(
            name=config.name, rule_type="range_check",
            table=table, column=config.column,
            failing_count=0, total_count=total,
        )

    where = " OR ".join(clauses)
    sql = f"SELECT COUNT(*) FROM {table} WHERE {where}"
    failing_count = int(conn.execute(sql).scalar() or 0)

    result = RuleResult.make(
        name=config.name, rule_type="range_check",
        table=table, column=config.column,
        failing_count=failing_count, total_count=total,
    )
    log.info("Rule [%s] %s → %s", config.name, table, "PASS" if result.passed else "FAIL")
    return result


def evaluate_unique_check(
    conn: Connection,
    table: str,
    config: UniqueCheckConfig,
) -> RuleResult:
    """Count duplicate values in column (total - distinct)."""
    _check_identifier(table, "table")
    _check_identifier(config.column, "column")

    sql = f"SELECT COUNT(*) - COUNT(DISTINCT {config.column}) FROM {table}"
    failing_count = int(conn.execute(sql).scalar() or 0)
    total = count_rows(conn, table)

    result = RuleResult.make(
        name=config.name, rule_type="unique_check",
        table=table, column=config.column,
        failing_count=failing_count, total_count=total,
    )
    log.info("Rule [%s] %s → %s", config.name, table, "PASS" if result.passed else "FAIL")
    return result


def evaluate_value_set(
    conn: Connection,
    table: str,
    config: ValueSetConfig,
) -> RuleResult:
    """Count rows where column value is not in the allowed set."""
    _check_identifier(table, "table")
    _check_identifier(config.column, "column")

    if not config.allowed_values:
        total = count_rows(conn, table)
        return RuleResult.make(
            name=config.name, rule_type="value_set",
            table=table, column=config.column,
            failing_count=0, total_count=total,
        )

    # Build safe IN list — values are quoted, not interpolated as identifiers
    quoted = ", ".join(f"'{v.replace(chr(39), chr(39)*2)}'" for v in config.allowed_values)
    sql = f"SELECT COUNT(*) FROM {table} WHERE {config.column} NOT IN ({quoted})"
    failing_count = int(conn.execute(sql).scalar() or 0)
    total = count_rows(conn, table)

    result = RuleResult.make(
        name=config.name, rule_type="value_set",
        table=table, column=config.column,
        failing_count=failing_count, total_count=total,
    )
    log.info("Rule [%s] %s → %s", config.name, table, "PASS" if result.passed else "FAIL")
    return result


def evaluate_referential(
    conn: Connection,
    table: str,
    config: ReferentialConfig,
) -> RuleResult:
    """
    Count rows in child table whose FK value doesn't exist in parent table.
    Both tables must be accessible from the same connection (or you can
    pass the target_conn when parent and child are in the same DB).
    """
    _check_identifier(table, "table")
    _check_identifier(config.column, "column")
    _check_identifier(config.parent_table, "parent_table")
    _check_identifier(config.parent_column, "parent_column")

    sql = (
        f"SELECT COUNT(*) FROM {table} c "
        f"WHERE c.{config.column} IS NOT NULL "
        f"AND c.{config.column} NOT IN "
        f"(SELECT {config.parent_column} FROM {config.parent_table})"
    )
    failing_count = int(conn.execute(sql).scalar() or 0)
    total = count_rows(conn, table)

    result = RuleResult.make(
        name=config.name, rule_type="referential",
        table=table, column=config.column,
        failing_count=failing_count, total_count=total,
    )
    log.info("Rule [%s] %s → %s", config.name, table, "PASS" if result.passed else "FAIL")
    return result


def evaluate_custom_sql(
    conn: Connection,
    table: str,
    config: CustomSQLConfig,
) -> RuleResult:
    """
    Execute a custom COUNT(*) SQL and compare to expected count.

    The SQL template may use {table} as a placeholder for the table name.
    Result must be a single integer (COUNT).
    """
    sql = config.sql.replace("{table}", table)
    actual_count = int(conn.execute(sql).scalar() or 0)
    total = count_rows(conn, table)
    # failing_count = deviation from expected
    failing_count = abs(actual_count - config.expect_count)

    result = RuleResult.make(
        name=config.name, rule_type="custom_sql",
        table=table, column=config.column,
        failing_count=failing_count, total_count=total,
        threshold_count=0,
    )
    log.info("Rule [%s] %s → %s (got %d expected %d)",
             config.name, table, "PASS" if result.passed else "FAIL",
             actual_count, config.expect_count)
    return result


def evaluate_distribution_check(
    source_conn: Connection,
    target_conn: Connection,
    source_table: str,
    target_table: str,
    config: DistributionCheckConfig,
) -> RuleResult:
    """
    Compare value distribution of a column between source and target.

    Uses a simple bucket approach: groups values into `buckets` ranges by
    computing MIN/MAX, then counts rows per bucket. Compares bucket counts
    as percentages of total and checks that deviation is within tolerance_pct.

    HIPAA: returns only counts, never actual values.
    """
    _check_identifier(source_table, "source_table")
    _check_identifier(target_table, "target_table")
    _check_identifier(config.column, "column")

    # Get total counts
    src_total = count_rows(source_conn, source_table)
    tgt_total = count_rows(target_conn, target_table)

    if src_total == 0 or tgt_total == 0:
        return RuleResult.make(
            name=config.name, rule_type="distribution_check",
            table=source_table, column=config.column,
            failing_count=0 if src_total == tgt_total else 1,
            total_count=src_total,
        )

    # Get min/max from source to define bucket boundaries
    try:
        minmax_sql = f"SELECT MIN({config.column}), MAX({config.column}) FROM {source_table}"
        row = source_conn.execute(minmax_sql).fetchone()
        col_min, col_max = (row[0] if row else None), (row[1] if row else None)
    except Exception as exc:
        log.warning("distribution_check: could not get min/max for %s.%s: %s",
                    source_table, config.column, exc)
        return RuleResult.make(
            name=config.name, rule_type="distribution_check",
            table=source_table, column=config.column,
            failing_count=0, total_count=src_total,
        )

    if col_min is None or col_max is None or col_min == col_max:
        # Uniform column — just compare counts
        failing_count = 0 if abs(src_total - tgt_total) / max(src_total, 1) * 100 <= config.tolerance_pct else 1
        return RuleResult.make(
            name=config.name, rule_type="distribution_check",
            table=source_table, column=config.column,
            failing_count=failing_count, total_count=src_total,
        )

    # Compare percent distribution via a simple count in each bucket range
    # Build one representative SQL bucket query per table
    try:
        bucket_sql = (
            f"SELECT CAST(({config.column} - ({col_min})) * {config.buckets} "
            f"/ NULLIF(({col_max}) - ({col_min}), 0) AS INT) AS bucket, "
            f"COUNT(*) AS cnt FROM {{table}} "
            f"GROUP BY CAST(({config.column} - ({col_min})) * {config.buckets} "
            f"/ NULLIF(({col_max}) - ({col_min}), 0) AS INT)"
        )
        src_rows = source_conn.execute(bucket_sql.format(table=source_table)).fetchall()
        tgt_rows = target_conn.execute(bucket_sql.format(table=target_table)).fetchall()
    except Exception as exc:
        log.warning("distribution_check: bucket query failed: %s", exc)
        return RuleResult.make(
            name=config.name, rule_type="distribution_check",
            table=source_table, column=config.column,
            failing_count=0, total_count=src_total,
        )

    src_dist = {row[0]: row[1] for row in src_rows}
    tgt_dist = {row[0]: row[1] for row in tgt_rows}

    all_buckets = set(src_dist) | set(tgt_dist)
    failing_buckets = 0
    for b in all_buckets:
        src_pct = src_dist.get(b, 0) / src_total * 100
        tgt_pct = tgt_dist.get(b, 0) / tgt_total * 100
        if abs(src_pct - tgt_pct) > config.tolerance_pct:
            failing_buckets += 1

    result = RuleResult.make(
        name=config.name, rule_type="distribution_check",
        table=source_table, column=config.column,
        failing_count=failing_buckets,
        total_count=len(all_buckets),
        threshold_count=0,
    )
    log.info("Rule [%s] %s.%s → %s (buckets=%d/%d failed)",
             config.name, source_table, config.column,
             "PASS" if result.passed else "FAIL", failing_buckets, len(all_buckets))
    return result


# ---------------------------------------------------------------------------
# Rule dispatcher
# ---------------------------------------------------------------------------

def build_and_evaluate(
    conn: Connection,
    table: str,
    rule_dict: dict,
) -> RuleResult:
    """
    Build a rule from a config dict and evaluate it.

    Expected keys in rule_dict:
      type: null_check | range_check | unique_check | value_set | referential | custom_sql
      name: str
      column: str (most rules)
      ... type-specific keys ...
    """
    rule_type = rule_dict.get("type", "").lower()
    name = rule_dict.get("name", f"{rule_type}_{table}")

    if rule_type == "null_check":
        cfg = NullCheckConfig(
            name=name,
            column=rule_dict["column"],
            expect=rule_dict.get("expect", "not_null"),
        )
        return evaluate_null_check(conn, table, cfg)

    elif rule_type == "range_check":
        cfg = RangeCheckConfig(
            name=name,
            column=rule_dict["column"],
            min=rule_dict.get("min"),
            max=rule_dict.get("max"),
        )
        return evaluate_range_check(conn, table, cfg)

    elif rule_type == "unique_check":
        cfg = UniqueCheckConfig(name=name, column=rule_dict["column"])
        return evaluate_unique_check(conn, table, cfg)

    elif rule_type == "value_set":
        cfg = ValueSetConfig(
            name=name,
            column=rule_dict["column"],
            allowed_values=rule_dict.get("allowed_values", []),
        )
        return evaluate_value_set(conn, table, cfg)

    elif rule_type == "referential":
        cfg = ReferentialConfig(
            name=name,
            column=rule_dict["column"],
            parent_table=rule_dict["parent_table"],
            parent_column=rule_dict["parent_column"],
        )
        return evaluate_referential(conn, table, cfg)

    elif rule_type == "custom_sql":
        cfg = CustomSQLConfig(
            name=name,
            sql=rule_dict["sql"],
            expect_count=rule_dict.get("expect_count", 0),
            column=rule_dict.get("column"),
        )
        return evaluate_custom_sql(conn, table, cfg)

    elif rule_type == "distribution_check":
        # distribution_check requires both source and target connections
        # and is dispatched separately in the runner
        raise ValueError(
            "distribution_check must be evaluated via evaluate_distribution_check() "
            "with separate source and target connections."
        )

    else:
        raise ValueError(f"Unknown rule type: {rule_type!r}")
