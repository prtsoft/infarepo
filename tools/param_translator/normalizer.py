"""
Parameter value normalizer.

For each classified parameter, produces:
  normalized_value  — clean, canonical form (ISO date, lowercase bool, etc.)
  spark_value       — value usable in PySpark / Glue context
  glue_arg_name     — --ARG_NAME string for getResolvedOptions
  notes             — list of human-readable translation notes

Date normalization:
  All date literals → ISO 8601: YYYY-MM-DD or YYYY-MM-DD HH:MM:SS
  Month abbreviations (JAN → 01) resolved.

Date mask normalization:
  PC/Oracle format → Java SimpleDateFormat (used by Spark to_date / date_format)
  e.g.  MM/DD/YYYY        → MM/dd/yyyy
        YYYY-MM-DD HH24:MI:SS → yyyy-MM-dd HH:mm:ss
        DD-MON-YYYY       → dd-MMM-yyyy

SQL normalization:
  Multi-line SQL joined to single line (with whitespace collapsed).
  Passed through expr_translator.translate_filter for PySpark spark_value.
  If translation confidence is LOW, spark_value is left as the raw SQL
  with a TODO comment.

Boolean normalization:
  Y/YES/TRUE/1/ON/ENABLED → "true"
  N/NO/FALSE/0/OFF/DISABLED → "false"

Path normalization:
  $PMRootDir, $PMCacheDir, etc. → placeholder S3 paths.
  UNC/Windows paths → note to migrate to S3.
"""

from __future__ import annotations
import re
import logging
from datetime import datetime
from typing import List, Optional

from .models import ParamType, PrmParameter

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Month abbreviation map
# ---------------------------------------------------------------------------

_MONTH_ABBR = {
    "JAN": "01", "FEB": "02", "MAR": "03", "APR": "04",
    "MAY": "05", "JUN": "06", "JUL": "07", "AUG": "08",
    "SEP": "09", "OCT": "10", "NOV": "11", "DEC": "12",
}

# ---------------------------------------------------------------------------
# Date mask token translation: PC/Oracle → Java SimpleDateFormat
# ---------------------------------------------------------------------------

# Ordered longest-first so YYYY matches before YY, HH24 before HH, etc.
_MASK_TOKEN_MAP = [
    ("YYYY",  "yyyy"),
    ("YY",    "yy"),
    ("MONTH", "MMMM"),
    ("MON",   "MMM"),
    ("MM",    "MM"),
    ("DD",    "dd"),
    ("DAY",   "EEEE"),
    ("DY",    "EEE"),
    ("HH24",  "HH"),
    ("HH12",  "hh"),
    ("HH",    "HH"),
    ("MI",    "mm"),
    ("SS",    "ss"),
    ("AM",    "a"),
    ("PM",    "a"),
    ("SSSSS", "SSSSS"),   # seconds since midnight (no Java equiv, keep)
    ("FF",    "SSS"),     # fractional seconds
    ("TZH",   "XXX"),     # timezone hours
    ("TZM",   "XXX"),     # timezone minutes
]

# PC path variable → S3 placeholder comment
_PC_PATH_VARS = {
    "$PMROOTDIR":    "s3://<your-bucket>/infa/root",
    "$PMBADFILEDIR": "s3://<your-bucket>/infa/bad",
    "$PMCACHEDIR":   "/tmp/glue-cache",   # local temp in Glue
    "$PMLOOKUPFILEDIR": "/tmp/glue-lookup",
    "$PMTARGETFILEDIR": "s3://<your-bucket>/output",
    "$PMSOURCEDIR":  "s3://<your-bucket>/input",
    "$PMSESSIONLOGDIR": "s3://<your-bucket>/logs/sessions",
    "$PMWORKFLOWLOGDIR": "s3://<your-bucket>/logs/workflows",
}


# ---------------------------------------------------------------------------
# Individual normalizers
# ---------------------------------------------------------------------------

def _normalize_date(value: str) -> tuple[str, str, List[str]]:
    """Returns (iso_value, spark_value, notes)."""
    v = value.strip()
    notes = []

    # ISO with time
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}:\d{2}(:\d{2})?)$", v)
    if m:
        iso = f"{m.group(1)}-{m.group(2)}-{m.group(3)} {m.group(4)}"
        if len(m.group(4)) == 5:
            iso += ":00"
        return iso, f'"{iso}"', []

    # ISO date only
    if re.match(r"^\d{4}-\d{2}-\d{2}$", v):
        return v, f'"{v}"', []

    # US: MM/DD/YYYY
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", v)
    if m:
        iso = f"{m.group(3)}-{m.group(1).zfill(2)}-{m.group(2).zfill(2)}"
        notes.append(f"Converted MM/DD/YYYY → ISO 8601: {iso}")
        return iso, f'"{iso}"', notes

    # EU: DD-MM-YYYY or DD.MM.YYYY
    m = re.match(r"^(\d{1,2})[-.](\d{1,2})[-.](\d{4})$", v)
    if m:
        iso = f"{m.group(3)}-{m.group(2).zfill(2)}-{m.group(1).zfill(2)}"
        notes.append(f"Converted DD-MM-YYYY → ISO 8601: {iso}")
        return iso, f'"{iso}"', notes

    # Oracle: DD-MON-YYYY or DD-MON-YY
    m = re.match(r"^(\d{1,2})-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-(\d{2,4})$",
                 v, re.IGNORECASE)
    if m:
        day = m.group(1).zfill(2)
        mon = _MONTH_ABBR[m.group(2).upper()]
        year = m.group(3)
        if len(year) == 2:
            year = ("20" if int(year) < 70 else "19") + year
            notes.append(f"Two-digit year expanded: {m.group(3)} → {year}")
        iso = f"{year}-{mon}-{day}"
        notes.append(f"Converted DD-MON-YYYY → ISO 8601: {iso}")
        return iso, f'"{iso}"', notes

    # 8-digit compact: YYYYMMDD
    m = re.match(r"^(\d{4})(\d{2})(\d{2})$", v)
    if m:
        iso = f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
        notes.append(f"Converted YYYYMMDD → ISO 8601: {iso}")
        return iso, f'"{iso}"', notes

    # Year-Month only
    m = re.match(r"^(\d{4})[-/](\d{2})$", v)
    if m:
        iso = f"{m.group(1)}-{m.group(2)}-01"
        notes.append(f"Year-Month only: defaulted day to 01 → {iso}")
        return iso, f'"{iso}"', notes

    notes.append(f"Unrecognised date format: '{v}' — verify manually")
    return v, f'"{v}"', notes


def _normalize_date_mask(value: str) -> tuple[str, str, List[str]]:
    """Returns (pc_mask, java_mask_for_spark, notes)."""
    result = value.strip()
    for pc_token, java_token in _MASK_TOKEN_MAP:
        result = re.sub(re.escape(pc_token), java_token, result, flags=re.IGNORECASE)
    notes = [
        f"PC/Oracle date mask → Java SimpleDateFormat: '{value}' → '{result}'",
        "Use in Spark: F.to_date(col, format) or F.date_format(col, format)",
    ]
    return value.strip(), result, notes


def _normalize_sql(value: str) -> tuple[str, str, List[str]]:
    """Returns (normalized_sql, spark_filter_expr, notes)."""
    # Collapse multi-line: join lines, normalize whitespace
    normalized = " ".join(value.split())
    notes = []
    if "\n" in value:
        notes.append("Multi-line SQL joined to single line")

    # Attempt PySpark translation via expr_translator
    spark_expr = normalized
    try:
        import sys
        from pathlib import Path as _Path
        sys.path.insert(0, str(_Path(__file__).parent.parent))
        from glue_gen.expr_translator import translate_filter, Confidence
        result = translate_filter(normalized)
        if result.confidence != Confidence.LOW:
            spark_expr = result.pyspark_expr
            if result.notes:
                notes.extend(result.notes)
            notes.append(
                f"Auto-translated to PySpark (confidence: {result.confidence}). "
                "Verify column names match DataFrame schema."
            )
        else:
            spark_expr = f"# TODO: translate manually: {normalized}"
            notes.append(
                "SQL expression could not be auto-translated (LOW confidence). "
                "Translate manually or use spark.sql() with a temp view."
            )
            notes.extend(result.notes)
    except ImportError:
        notes.append("glue_gen not available — spark_value left as raw SQL")
        spark_expr = normalized

    return normalized, spark_expr, notes


def _normalize_boolean(value: str) -> tuple[str, str, List[str]]:
    true_vals  = {"y", "yes", "true", "1", "on", "enabled"}
    false_vals = {"n", "no", "false", "0", "off", "disabled"}
    v = value.strip().lower()
    if v in true_vals:
        return "true", "True", []
    if v in false_vals:
        return "false", "False", []
    return value.strip(), value.strip(), [f"Ambiguous boolean value: '{value}'"]


def _normalize_path(value: str) -> tuple[str, str, List[str]]:
    v = value.strip()
    notes = []

    # PC built-in path variables
    for pc_var, s3_equiv in _PC_PATH_VARS.items():
        if v.upper().startswith(pc_var.upper()):
            suffix = v[len(pc_var):]
            normalized = s3_equiv + suffix
            notes.append(
                f"PC path variable '{pc_var}' → placeholder '{s3_equiv}'. "
                "Update with actual S3 path."
            )
            return normalized, f'"{normalized}"', notes

    # Windows/UNC paths
    if re.match(r"^([A-Za-z]:\\|\\\\)", v):
        notes.append(
            f"Windows/UNC path: '{v}'. "
            "Migrate source file to S3 and update this value."
        )
        return v, f'"s3://<your-bucket>/{v.replace(chr(92), "/").lstrip("/")}  # TODO"', notes

    # Already S3
    if v.startswith("s3://"):
        return v, f'"{v}"', []

    return v, f'"{v}"', notes


# ---------------------------------------------------------------------------
# Main normalizer
# ---------------------------------------------------------------------------

def normalize_param(param: PrmParameter) -> None:
    """
    Normalize a PrmParameter in-place.
    Sets normalized_value, spark_value, glue_arg_name, and notes.
    """
    from .classifier import classify_notes

    # glue_arg_name: strip leading $, uppercase, replace non-alphanumeric with _
    param.glue_arg_name = re.sub(r"[^A-Z0-9_]", "_", param.name.upper()).strip("_")

    # Merge classification notes
    param.notes = classify_notes(param)

    v = param.raw_value.strip()
    t = param.param_type

    if t == ParamType.EMPTY:
        param.normalized_value = ""
        param.spark_value = '""'

    elif t == ParamType.BOOLEAN:
        param.normalized_value, param.spark_value, extra = _normalize_boolean(v)
        param.notes.extend(extra)

    elif t == ParamType.INTEGER:
        param.normalized_value = v
        param.spark_value = v

    elif t == ParamType.DECIMAL:
        param.normalized_value = v
        param.spark_value = v

    elif t == ParamType.DATE:
        param.normalized_value, param.spark_value, extra = _normalize_date(v)
        param.notes.extend(extra)

    elif t == ParamType.DATE_MASK:
        param.normalized_value, param.spark_value, extra = _normalize_date_mask(v)
        param.notes.extend(extra)

    elif t == ParamType.SQL:
        param.normalized_value, param.spark_value, extra = _normalize_sql(v)
        param.notes.extend(extra)

    elif t == ParamType.PATH:
        param.normalized_value, param.spark_value, extra = _normalize_path(v)
        param.notes.extend(extra)

    else:  # STRING
        param.normalized_value = v
        param.spark_value = f'"{v}"'


def normalize_file(prm_file) -> None:
    """Normalize all parameters in a PrmFile in-place."""
    for section in prm_file.sections:
        for param in section.params.values():
            normalize_param(param)
