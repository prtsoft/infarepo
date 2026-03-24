"""
Parameter value type classifier.

Scoring approach: each candidate type accumulates evidence points.
The type with the highest score wins. Ties broken by priority order.

Types detected:
  EMPTY      - blank or whitespace-only value
  BOOLEAN    - Y/N/YES/NO/TRUE/FALSE/1/0  (exact, case-insensitive)
  INTEGER    - whole number, optional sign
  DECIMAL    - decimal number, optional sign
  DATE       - recognisable date literal in any common format
  DATE_MASK  - format string like MM/DD/YYYY, YYYY-MM-DD HH24:MI:SS, etc.
  SQL        - SQL fragment: SELECT/FROM/WHERE/JOIN/INSERT/UPDATE/DELETE/WITH
  PATH       - file system or UNC path
  STRING     - fallback

DATE vs DATE_MASK distinction:
  A DATE has actual numeric day/month/year.
  A DATE_MASK has format tokens like MM, DD, YYYY, HH24, MI, SS.

SQL scoring uses keyword density — a value needs multiple SQL keywords
or one dominant keyword (SELECT or FROM) to score as SQL.  This prevents
short strings like "FROM_DATE" from misclassifying.
"""

from __future__ import annotations
import re
from typing import List, Tuple

from .models import ParamType, PrmParameter


# ---------------------------------------------------------------------------
# Patterns
# ---------------------------------------------------------------------------

_BOOLEAN_VALUES = {
    "y", "n", "yes", "no", "true", "false", "1", "0",
    "on", "off", "enabled", "disabled",
}

# Date literals — ordered from most specific to least
_DATE_PATTERNS: List[Tuple[re.Pattern, str]] = [
    # ISO 8601 with time:  2024-01-15 14:30:00  or  2024-01-15T14:30:00
    (re.compile(r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?$"), "YYYY-MM-DD HH:MM:SS"),
    # ISO date:            2024-01-15
    (re.compile(r"^\d{4}-\d{2}-\d{2}$"), "YYYY-MM-DD"),
    # US date:             01/15/2024  or  1/5/2024
    (re.compile(r"^\d{1,2}/\d{1,2}/\d{4}$"), "MM/DD/YYYY"),
    # EU date:             15-01-2024  or  15.01.2024
    (re.compile(r"^\d{1,2}[-\.]\d{1,2}[-\.]\d{4}$"), "DD-MM-YYYY"),
    # Oracle style:        15-JAN-2024  or  15-JAN-24
    (re.compile(r"^\d{1,2}-(JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)-\d{2,4}$",
                re.IGNORECASE), "DD-MON-YYYY"),
    # Year-Month only:     2024-01  or  2024/01
    (re.compile(r"^\d{4}[-/]\d{2}$"), "YYYY-MM"),
    # 8-digit compact:     20240115
    (re.compile(r"^\d{8}$"), "YYYYMMDD"),
]

# Date mask tokens — these appear in FORMAT STRINGS, not date values
_DATE_MASK_TOKENS = re.compile(
    r"\b(YYYY|YY|MM|MON|MONTH|DD|DY|DAY|HH24|HH12|HH|MI|SS|AM|PM|"
    r"yyyy|mm|dd|hh|mi|ss)\b"
)

# SQL keywords that indicate a SQL value
_SQL_KEYWORDS = re.compile(
    r"\b(SELECT|FROM|WHERE|JOIN|LEFT|RIGHT|INNER|OUTER|FULL|CROSS|"
    r"INSERT|UPDATE|DELETE|MERGE|UPSERT|TRUNCATE|WITH|UNION|INTERSECT|"
    r"EXCEPT|GROUP\s+BY|ORDER\s+BY|HAVING|DISTINCT|TOP|LIMIT|OFFSET|"
    r"EXISTS|NOT\s+EXISTS|IN\s*\(|CASE\s+WHEN|THEN|END|OVER\s*\(|"
    r"PARTITION\s+BY|DATEADD|DATEDIFF|GETDATE|SYSDATE|CAST\s*\(|"
    r"CONVERT\s*\(|COALESCE\s*\(|ISNULL\s*\(|NVL\s*\()\b",
    re.IGNORECASE,
)

# Dominant SQL keywords that alone strongly indicate SQL
_SQL_DOMINANT = re.compile(
    r"\bSELECT\b|\bFROM\b\s+\w|\bWHERE\b\s+\w|\bJOIN\b\s+\w",
    re.IGNORECASE,
)

# Filter condition patterns: comparison ops + logical connectors → SQL WHERE clause
_SQL_FILTER = re.compile(
    r"(?:!=|<>)\s*.+\s+(?:AND|OR)\b"     # !=  or <> combined with AND/OR
    r"|\bIS\s+(?:NOT\s+)?NULL\b"          # IS NULL / IS NOT NULL
    r"|\bBETWEEN\b.+\bAND\b"             # BETWEEN x AND y
    r"|\bNOT\s+IN\s*\(",                  # NOT IN (...)
    re.IGNORECASE,
)

# File system paths
_PATH_PATTERNS = [
    re.compile(r"^[A-Za-z]:\\"),                     # Windows: C:\...
    re.compile(r"^\\\\"),                             # UNC: \\server\share
    re.compile(r"^/[a-zA-Z0-9_\-./]+"),              # Unix: /data/input/
    re.compile(r"^[A-Za-z]:/"),                       # Windows forward slash: C:/...
    re.compile(r"^s3://"),                            # S3 URI
    re.compile(r"^(hdfs|abfs|gs)://"),                # Other cloud paths
    re.compile(r"^\$PM[A-Z]"),                        # PC variables: $PMRootDir, $PMCacheDir
]

_NUMBER_RE = re.compile(r"^[+-]?\d+$")
_DECIMAL_RE = re.compile(r"^[+-]?\d*\.\d+([eE][+-]?\d+)?$")


# ---------------------------------------------------------------------------
# Classifier
# ---------------------------------------------------------------------------

def classify(value: str) -> str:
    """Return the ParamType for a raw parameter value string."""
    v = value.strip()

    if not v:
        return ParamType.EMPTY

    if v.lower() in _BOOLEAN_VALUES:
        return ParamType.BOOLEAN

    for pattern, _ in _DATE_PATTERNS:
        if pattern.match(v):
            return ParamType.DATE

    if _NUMBER_RE.match(v):
        return ParamType.INTEGER

    if _DECIMAL_RE.match(v):
        return ParamType.DECIMAL

    if _is_date_mask(v):
        return ParamType.DATE_MASK

    if _is_path(v):
        return ParamType.PATH

    if _is_sql(v):
        return ParamType.SQL

    return ParamType.STRING


def _is_date_mask(value: str) -> bool:
    """
    True if the value looks like a date/time format string.
    Must contain at least 2 recognised format tokens AND no spaces
    that aren't between tokens (i.e. not a sentence).
    """
    tokens = _DATE_MASK_TOKENS.findall(value)
    if len(tokens) < 2:
        return False
    # The value should be mostly format tokens and separators, not English words
    # Strip tokens and separators — what's left should be minimal
    remainder = _DATE_MASK_TOKENS.sub("", value)
    remainder = re.sub(r"[-/: .,T]", "", remainder)
    # If more than 4 non-token non-separator chars remain, probably not a mask
    return len(remainder) <= 4


def _is_path(value: str) -> bool:
    return any(p.match(value) for p in _PATH_PATTERNS)


def _is_sql(value: str) -> bool:
    """
    True if the value contains enough SQL keyword evidence.
    Single short strings that happen to contain FROM or WHERE
    (e.g. column names) should NOT classify as SQL.
    """
    # Multi-line values are almost certainly SQL
    if "\n" in value:
        kw_count = len(_SQL_KEYWORDS.findall(value))
        return kw_count >= 1

    # Single line: need dominant keyword + min length, OR multiple keywords
    if len(value) < 10:
        return False

    dominant = bool(_SQL_DOMINANT.search(value))
    kw_count = len(_SQL_KEYWORDS.findall(value))

    if dominant and kw_count >= 2:
        return True
    if kw_count >= 3:
        return True
    if _SQL_FILTER.search(value):
        return True

    return False


# ---------------------------------------------------------------------------
# Classify all params in a PrmFile
# ---------------------------------------------------------------------------

def classify_file(prm_file) -> None:
    """Classify all parameters in a PrmFile in-place."""
    for section in prm_file.sections:
        for param in section.params.values():
            param.param_type = classify(param.raw_value)


def classify_notes(param: PrmParameter) -> List[str]:
    """Return human-readable notes about why a value was classified as its type."""
    notes = []
    v = param.raw_value.strip()

    if param.param_type == ParamType.DATE:
        for pattern, fmt in _DATE_PATTERNS:
            if pattern.match(v):
                notes.append(f"Detected date format: {fmt}")
                break

    elif param.param_type == ParamType.DATE_MASK:
        tokens = _DATE_MASK_TOKENS.findall(v)
        notes.append(f"Detected date mask tokens: {', '.join(tokens)}")

    elif param.param_type == ParamType.SQL:
        kws = list(dict.fromkeys(
            m.upper().split()[0]
            for m in _SQL_KEYWORDS.findall(v)
        ))[:5]
        notes.append(f"SQL keywords detected: {', '.join(kws)}")
        if "\n" in v:
            notes.append("Multi-line SQL value — joined from continuation lines")

    elif param.param_type == ParamType.PATH:
        notes.append("Filesystem/cloud path — verify S3 equivalent path is set")

    elif param.param_type == ParamType.BOOLEAN:
        notes.append(f"Boolean value: '{v}' — will be normalised to true/false")

    return notes
