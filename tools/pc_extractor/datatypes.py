"""
PowerCenter → PySpark / Delta data type mapping.

Central module so every code generator uses the same type translations.
"""
from __future__ import annotations
import re


# ---------------------------------------------------------------------------
# PC datatype → PySpark DDL string
# ---------------------------------------------------------------------------

# Each key is a normalized (lowercase, stripped) PC datatype name.
# Values are PySpark DDL strings (what you'd pass to F.col(...).cast("...")).
_TYPE_MAP: dict[str, str] = {
    # ── String types ──────────────────────────────────────────────────────
    "nvarchar":            "string",
    "varchar":             "string",
    "char":                "string",
    "nchar":               "string",
    "text":                "string",
    "ntext":               "string",
    "clob":                "string",
    "long":                "string",        # Oracle LONG
    "xmltype":             "string",

    # ── Integer types ─────────────────────────────────────────────────────
    "int":                 "integer",
    "integer":             "integer",
    "smallint":            "short",
    "tinyint":             "byte",
    "bigint":              "long",
    "int identity":        "long",

    # ── Decimal / money types ─────────────────────────────────────────────
    "decimal":             "decimal",       # caller appends (p,s) if needed
    "numeric":             "decimal",
    "number":              "decimal",       # Oracle NUMBER
    "money":               "decimal(19,4)",
    "smallmoney":          "decimal(10,4)",
    "float":               "double",
    "real":                "float",
    "double":              "double",
    "double precision":    "double",

    # ── Date / time types ─────────────────────────────────────────────────
    "date":                "date",
    "datetime":            "timestamp",
    "datetime2":           "timestamp",
    "smalldatetime":       "timestamp",
    "datetimeoffset":      "timestamp",
    "time":                "string",        # PySpark has no pure time type
    "timestamp":           "timestamp",

    # ── Boolean ───────────────────────────────────────────────────────────
    "bit":                 "boolean",
    "boolean":             "boolean",

    # ── Binary ────────────────────────────────────────────────────────────
    "varbinary":           "binary",
    "binary":              "binary",
    "image":               "binary",
    "blob":                "binary",
    "raw":                 "binary",        # Oracle RAW
    "long raw":            "binary",

    # ── UUID / uniqueidentifier ───────────────────────────────────────────
    # Mapped to string; callers should add lowercase cast + UUID validation
    "uniqueidentifier":    "string",
    "guid":                "string",
}

# Regex matching column names that are likely uniqueidentifiers (for validation hints)
UNIQUEIDENTIFIER_COLS = re.compile(
    r"\b(guid|uuid|uniqueidentifier|rowguid)\b",
    re.IGNORECASE,
)


def map_type(pc_type: str, precision: int = 0, scale: int = 0) -> str:
    """
    Map a PowerCenter datatype name to a PySpark DDL type string.

    For decimal/numeric types with explicit precision and scale, returns
    "decimal(p,s)" when both are non-zero.

    Unknown types fall back to "string" with no error.

    Examples:
        map_type("nvarchar")          → "string"
        map_type("decimal", 18, 4)    → "decimal(18,4)"
        map_type("datetime2")         → "timestamp"
        map_type("uniqueidentifier")  → "string"
        map_type("some_new_type")     → "string"
    """
    normalized = pc_type.strip().lower()

    # Strip precision/scale suffix from type string if present, e.g. "varchar(50)"
    normalized = re.sub(r"\s*\(\s*[\d,\s]+\s*\)", "", normalized).strip()

    spark_type = _TYPE_MAP.get(normalized, "string")

    # Append precision/scale for decimal types when caller provides them
    if spark_type == "decimal" and precision > 0:
        if scale > 0:
            spark_type = f"decimal({precision},{scale})"
        else:
            spark_type = f"decimal({precision},0)"

    return spark_type


def is_uniqueidentifier(pc_type: str, column_name: str = "") -> bool:
    """
    Return True if the type or column name suggests a UUID / uniqueidentifier.
    Used to add lowercase cast and validation hints in generated code.
    """
    if pc_type.strip().lower() in ("uniqueidentifier", "guid"):
        return True
    return bool(UNIQUEIDENTIFIER_COLS.search(column_name))
