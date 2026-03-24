"""
Row count reconciliation and schema diff.

HIPAA safety: only row counts and column metadata are retrieved.
No actual data values are read or stored.
"""
from __future__ import annotations

import logging
import re
from typing import Dict, List, Optional, Tuple

from .connections import Connection
from .models import ReconResult, SchemaField, SchemaDiff, TypeMismatch

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Type normalization
# ---------------------------------------------------------------------------

# Map dialect-specific types → normalized category
_TYPE_MAP: Dict[str, str] = {
    # String types
    "varchar": "string",     "nvarchar": "string",     "char": "string",
    "nchar": "string",       "text": "string",          "ntext": "string",
    "string": "string",      "clob": "string",
    # Integer types
    "int": "integer",        "integer": "integer",      "bigint": "integer",
    "smallint": "integer",   "tinyint": "integer",      "long": "integer",
    "short": "integer",
    # Decimal / float
    "decimal": "decimal",    "numeric": "decimal",      "float": "decimal",
    "double": "decimal",     "real": "decimal",         "money": "decimal",
    "smallmoney": "decimal",
    # Date / time
    "date": "date",
    "datetime": "timestamp", "datetime2": "timestamp",  "timestamp": "timestamp",
    "smalldatetime": "timestamp",
    # Boolean
    "bit": "boolean",        "boolean": "boolean",      "bool": "boolean",
    # Binary
    "binary": "binary",      "varbinary": "binary",     "image": "binary",
    "bytes": "binary",
}


def normalize_type(raw_type: str) -> str:
    """Normalize a database type string to a canonical category."""
    # Strip precision/scale: varchar(255) → varchar
    base = re.sub(r"\s*\(.*\)", "", raw_type.strip()).lower()
    # Handle "unsigned" suffix (MySQL)
    base = base.replace(" unsigned", "")
    return _TYPE_MAP.get(base, base)


# Compatible type pairs (order doesn't matter)
_COMPATIBLE_PAIRS = {
    frozenset({"string", "clob"}),
    frozenset({"integer", "decimal"}),
    frozenset({"timestamp", "date"}),
    frozenset({"boolean", "integer"}),   # bit → int is common
}


def types_compatible(a: str, b: str) -> bool:
    """Return True if two normalized types are considered migration-compatible."""
    if a == b:
        return True
    na, nb = normalize_type(a), normalize_type(b)
    if na == nb:
        return True
    pair = frozenset({na, nb})
    return pair in _COMPATIBLE_PAIRS


# ---------------------------------------------------------------------------
# Row count reconciliation
# ---------------------------------------------------------------------------

def count_rows(conn: Connection, table: str, where_clause: Optional[str] = None) -> int:
    """Execute COUNT(*) against a table, optionally filtered."""
    sql = f"SELECT COUNT(*) FROM {table}"
    if where_clause:
        sql += f" WHERE {where_clause}"
    result = conn.execute(sql)
    return int(result.scalar() or 0)


def run_recon(
    source_conn: Connection,
    target_conn: Connection,
    source_table: str,
    target_table: str,
    tolerance_pct: float = 1.0,
    source_sql: Optional[str] = None,
    target_sql: Optional[str] = None,
    notes: Optional[List[str]] = None,
) -> ReconResult:
    """
    Compare row counts between source and target tables.

    Args:
        source_conn:    Connection to source database.
        target_conn:    Connection to target database.
        source_table:   Fully qualified source table (e.g. "dbo.ORDERS").
        target_table:   Target table name (e.g. "orders").
        tolerance_pct:  Allowed delta as a percentage of source count.
        source_sql:     Optional override COUNT query for source.
        target_sql:     Optional override COUNT query for target.
        notes:          Additional notes to include in result.
    """
    table_pair = f"{source_table} → {target_table}"
    extra_notes = list(notes or [])

    if source_sql:
        source_count = int(source_conn.execute(source_sql).scalar() or 0)
        extra_notes.append(f"Source count via custom SQL")
    else:
        source_count = count_rows(source_conn, source_table)

    if target_sql:
        target_count = int(target_conn.execute(target_sql).scalar() or 0)
        extra_notes.append(f"Target count via custom SQL")
    else:
        target_count = count_rows(target_conn, target_table)

    delta = target_count - source_count
    delta_pct = (abs(delta) / source_count * 100) if source_count else 0.0
    passed = delta_pct <= tolerance_pct

    if not passed:
        extra_notes.append(
            f"Delta {delta:+d} rows ({delta_pct:.2f}%) exceeds tolerance {tolerance_pct}%"
        )
    if source_count == 0:
        extra_notes.append("Warning: source count is 0 — verify table is not empty")

    log.info(
        "Recon %s: source=%d target=%d delta=%+d (%.2f%%) %s",
        table_pair, source_count, target_count, delta, delta_pct,
        "PASS" if passed else "FAIL",
    )
    return ReconResult(
        table_pair=table_pair,
        source_table=source_table,
        target_table=target_table,
        source_count=source_count,
        target_count=target_count,
        delta=delta,
        delta_pct=delta_pct,
        tolerance_pct=tolerance_pct,
        passed=passed,
        notes=extra_notes,
    )


# ---------------------------------------------------------------------------
# Schema diff
# ---------------------------------------------------------------------------

# SQL to fetch column metadata — dialect-specific
_SCHEMA_SQL = {
    "sqlserver": """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA + '.' + TABLE_NAME = ?
           OR TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """,
    "default": """
        SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_NAME = ?
        ORDER BY ORDINAL_POSITION
    """,
}


def get_schema(
    conn: Connection,
    table: str,
    dialect: str = "default",
) -> List[SchemaField]:
    """
    Retrieve column schema from INFORMATION_SCHEMA.

    Falls back to empty list if the query fails (e.g. Databricks uses different catalog).
    """
    # For MockConnection, the caller pre-registers results
    sql = _SCHEMA_SQL.get(dialect, _SCHEMA_SQL["default"])
    # Simplify: use only table name (strip schema prefix)
    table_name = table.split(".")[-1]
    try:
        rows = conn.execute(sql.replace("?", f"'{table}'")).fetchall()
        if not rows:
            rows = conn.execute(sql.replace("?", f"'{table_name}'")).fetchall()
    except Exception as exc:
        log.warning("Could not fetch schema for %s: %s", table, exc)
        return []

    fields = []
    for row in rows:
        name = row[0]
        dtype = normalize_type(str(row[1]))
        nullable = str(row[2]).upper() not in ("NO", "N", "NOT NULL", "0")
        precision = int(row[3]) if len(row) > 3 and row[3] is not None else None
        scale = int(row[4]) if len(row) > 4 and row[4] is not None else None
        fields.append(SchemaField(name=name, dtype=dtype, nullable=nullable,
                                  precision=precision, scale=scale))
    return fields


def diff_schemas(
    source_conn: Connection,
    target_conn: Connection,
    source_table: str,
    target_table: str,
    ignored_columns: Optional[List[str]] = None,
    type_equivalences: Optional[List[Tuple[str, str]]] = None,
    source_dialect: str = "default",
    target_dialect: str = "default",
) -> SchemaDiff:
    """
    Compare column schemas between source and target tables.

    Args:
        ignored_columns:   Columns to ignore when diffing (e.g. audit columns added by ETL).
        type_equivalences: Extra pairs of types to treat as compatible.
    """
    ignored = {c.upper() for c in (ignored_columns or [])}

    # Build extra compatible pairs
    extra_compat: set = set()
    for a, b in (type_equivalences or []):
        extra_compat.add(frozenset({normalize_type(a), normalize_type(b)}))

    source_fields = get_schema(source_conn, source_table, source_dialect)
    target_fields = get_schema(target_conn, target_table, target_dialect)

    source_map = {f.name.upper(): f for f in source_fields}
    target_map = {f.name.upper(): f for f in target_fields}

    # Filter out ignored columns
    effective_source = {k: v for k, v in source_map.items() if k not in ignored}
    effective_target = {k: v for k, v in target_map.items() if k not in ignored}

    missing_in_target = sorted(set(effective_source) - set(effective_target))
    extra_in_target   = sorted(set(effective_target) - set(effective_source))
    common            = set(effective_source) & set(effective_target)

    type_mismatches: List[TypeMismatch] = []
    for col in sorted(common):
        st = effective_source[col].dtype
        tt = effective_target[col].dtype
        if st != tt:
            pair = frozenset({st, tt})
            compatible = types_compatible(st, tt) or pair in extra_compat
            type_mismatches.append(TypeMismatch(
                column=col, source_type=st, target_type=tt, compatible=compatible
            ))

    # Schema passes if no missing columns and no incompatible type mismatches
    passed = (
        len(missing_in_target) == 0
        and all(m.compatible for m in type_mismatches)
    )

    table_pair = f"{source_table} → {target_table}"
    log.info(
        "Schema diff %s: missing=%d extra=%d mismatches=%d %s",
        table_pair, len(missing_in_target), len(extra_in_target),
        len(type_mismatches), "PASS" if passed else "FAIL",
    )
    return SchemaDiff(
        table_pair=table_pair,
        source_table=source_table,
        target_table=target_table,
        source_columns=source_fields,
        target_columns=target_fields,
        missing_in_target=missing_in_target,
        extra_in_target=extra_in_target,
        type_mismatches=type_mismatches,
        ignored_columns=sorted(ignored),
        passed=passed,
    )
