"""
PowerCenter expression → PySpark (F.*) expression translator.

Handles the most common PC built-in functions encountered in
Expression, Filter, Lookup Condition, Join Condition, and Router transformations.

Returns a TranslationResult with:
  - pyspark_expr : translated string (may contain F.col(), F.lit(), F.when(), etc.)
  - confidence   : HIGH / MEDIUM / LOW
  - notes        : list of warnings / manual review flags

Strategy:
  1. Simple token replacements (SYSDATE, NULL, TRUE, FALSE)
  2. Function-by-function regex substitution (IIF, NVL, DECODE, etc.)
  3. Operator normalization (!= → !=, <> → !=, || → +)
  4. Column reference wrapping: bare identifiers → F.col("X")
  5. Anything left over that looks like an unknown function → LOW confidence

HIPAA note: this translator never logs expression content at INFO level.
  Raw expressions may contain column names that hint at PII fields.
  Use DEBUG only.
"""

from __future__ import annotations
import re
import logging
from dataclasses import dataclass, field
from typing import List

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------

class Confidence:
    HIGH   = "HIGH"
    MEDIUM = "MEDIUM"
    LOW    = "LOW"


@dataclass
class TranslationResult:
    pyspark_expr: str
    confidence: str = Confidence.HIGH
    notes: List[str] = field(default_factory=list)

    def downgrade(self, to: str, note: str) -> None:
        if to == Confidence.LOW or (to == Confidence.MEDIUM and self.confidence == Confidence.HIGH):
            self.confidence = to
        self.notes.append(note)


# ---------------------------------------------------------------------------
# Simple token map (case-insensitive whole-word replacements)
# ---------------------------------------------------------------------------

_TOKEN_MAP = {
    r"\bSYSDATE\b":           "F.current_timestamp()",
    r"\bCURRENT_DATE\b":      "F.current_date()",
    r"\bNULL\b":              "None",
    r"\bTRUE\b":              "True",
    r"\bFALSE\b":             "False",
    r"\bSPACES\b":            '" "',
    r"\|\|":                  " + ",        # string concat operator
    r"\s*<>\s*":              " != ",       # SQL not-equal
}

# ---------------------------------------------------------------------------
# Function translation patterns
# Each entry: (pattern, replacement_or_callable, confidence_impact)
# Applied in order — order matters for nested calls.
# ---------------------------------------------------------------------------

def _iif(m: re.Match) -> str:
    cond, true_val, false_val = m.group(1), m.group(2), m.group(3)
    return f"F.when({_translate_inner(cond)}, {_translate_inner(true_val)}).otherwise({_translate_inner(false_val)})"


def _nvl(m: re.Match) -> str:
    a, b = m.group(1), m.group(2)
    return f"F.coalesce({_translate_inner(a)}, {_translate_inner(b)})"


def _nvl2(m: re.Match) -> str:
    expr, not_null, is_null = m.group(1), m.group(2), m.group(3)
    return (
        f"F.when({_translate_inner(expr)}.isNotNull(), {_translate_inner(not_null)})"
        f".otherwise({_translate_inner(is_null)})"
    )


def _decode(m: re.Match) -> str:
    """DECODE(col, val1, res1, val2, res2, ..., default) → chained when/otherwise"""
    raw = m.group(1)
    parts = _split_args(raw)
    if len(parts) < 3:
        return f"# TODO: DECODE({raw})  -- translate manually"
    col_expr = _translate_inner(parts[0])
    chains = []
    i = 1
    while i + 1 < len(parts):
        val = _translate_inner(parts[i])
        res = _translate_inner(parts[i+1])
        chains.append(f"F.when({col_expr} == {val}, {res})")
        i += 2
    result = ".".join(chains)
    if i < len(parts):          # default value present
        result += f".otherwise({_translate_inner(parts[i])})"
    else:
        result += ".otherwise(None)"
    return result


def _in_list(m: re.Match) -> str:
    col = _translate_inner(m.group(1))
    vals = ", ".join(_translate_inner(v.strip()) for v in _split_args(m.group(2)))
    return f"{col}.isin([{vals}])"


def _not_in_list(m: re.Match) -> str:
    col = _translate_inner(m.group(1))
    vals = ", ".join(_translate_inner(v.strip()) for v in _split_args(m.group(2)))
    return f"~{col}.isin([{vals}])"


# Patterns: (compiled_regex, replacement_fn_or_str, confidence_on_match)
_FUNC_PATTERNS: list = [
    # IIF(condition, true_val, false_val)
    (re.compile(r"\bIIF\s*\((.+?),\s*(.+?),\s*(.+?)\)", re.IGNORECASE | re.DOTALL),
     _iif, Confidence.HIGH),

    # NVL(a, b)
    (re.compile(r"\bNVL\s*\((.+?),\s*(.+?)\)", re.IGNORECASE | re.DOTALL),
     _nvl, Confidence.HIGH),

    # NVL2(expr, not_null_val, null_val)
    (re.compile(r"\bNVL2\s*\((.+?),\s*(.+?),\s*(.+?)\)", re.IGNORECASE | re.DOTALL),
     _nvl2, Confidence.HIGH),

    # DECODE(col, v1, r1, ...)
    (re.compile(r"\bDECODE\s*\((.+?)\)", re.IGNORECASE | re.DOTALL),
     _decode, Confidence.MEDIUM),

    # IN(col, v1, v2, ...)  — PC uses IN() as function not SQL IN keyword
    (re.compile(r"\bIN\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)", re.IGNORECASE | re.DOTALL),
     _in_list, Confidence.HIGH),

    # NOT IN
    (re.compile(r"\bNOT\s+IN\s*\(\s*(.+?)\s*,\s*(.+?)\s*\)", re.IGNORECASE | re.DOTALL),
     _not_in_list, Confidence.HIGH),

    # SUBSTR / SUBSTRING(col, start, length)
    (re.compile(r"\bSUBSTR(?:ING)?\s*\(\s*(.+?)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)", re.IGNORECASE),
     lambda m: f"F.substring({_wrap_col(m.group(1))}, {m.group(2)}, {m.group(3)})",
     Confidence.HIGH),

    # SUBSTR(col, start)  — no length
    (re.compile(r"\bSUBSTR\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)", re.IGNORECASE),
     lambda m: f"F.substring({_wrap_col(m.group(1))}, {m.group(2)}, 2147483647)",
     Confidence.HIGH),

    # LENGTH(col)
    (re.compile(r"\bLENGTH\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.length({_wrap_col(m.group(1))})", Confidence.HIGH),

    # UPPER / LOWER
    (re.compile(r"\bUPPER\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.upper({_wrap_col(m.group(1))})", Confidence.HIGH),
    (re.compile(r"\bLOWER\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.lower({_wrap_col(m.group(1))})", Confidence.HIGH),

    # LTRIM / RTRIM / TRIM
    (re.compile(r"\bLTRIM\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.ltrim({_wrap_col(m.group(1))})", Confidence.HIGH),
    (re.compile(r"\bRTRIM\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.rtrim({_wrap_col(m.group(1))})", Confidence.HIGH),
    (re.compile(r"\bTRIM\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.trim({_wrap_col(m.group(1))})", Confidence.HIGH),

    # LPAD(col, len, pad) / RPAD
    (re.compile(r"\bLPAD\s*\(\s*(.+?)\s*,\s*(\d+)\s*,\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.lpad({_wrap_col(m.group(1))}, {m.group(2)}, {m.group(3)})",
     Confidence.HIGH),
    (re.compile(r"\bRPAD\s*\(\s*(.+?)\s*,\s*(\d+)\s*,\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.rpad({_wrap_col(m.group(1))}, {m.group(2)}, {m.group(3)})",
     Confidence.HIGH),

    # CONCAT(a, b, ...) — handled as binary via ||, but explicit CONCAT too
    (re.compile(r"\bCONCAT\s*\(\s*(.+?)\s*\)", re.IGNORECASE | re.DOTALL),
     lambda m: "F.concat(" + ", ".join(_translate_inner(a.strip()) for a in _split_args(m.group(1))) + ")",
     Confidence.HIGH),

    # ROUND(col, n)
    (re.compile(r"\bROUND\s*\(\s*(.+?)\s*,\s*(\d+)\s*\)", re.IGNORECASE),
     lambda m: f"F.round({_wrap_col(m.group(1))}, {m.group(2)})", Confidence.HIGH),
    (re.compile(r"\bROUND\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.round({_wrap_col(m.group(1))}, 0)", Confidence.HIGH),

    # ABS
    (re.compile(r"\bABS\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.abs({_wrap_col(m.group(1))})", Confidence.HIGH),

    # CEIL / FLOOR
    (re.compile(r"\bCEIL(?:ING)?\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.ceil({_wrap_col(m.group(1))})", Confidence.HIGH),
    (re.compile(r"\bFLOOR\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.floor({_wrap_col(m.group(1))})", Confidence.HIGH),

    # TRUNC(date) — date truncation
    (re.compile(r"\bTRUNC\s*\(\s*(.+?)\s*,\s*'([^']+)'\s*\)", re.IGNORECASE),
     lambda m: f"F.date_trunc('{m.group(2).lower()}', {_wrap_col(m.group(1))})",
     Confidence.HIGH),
    (re.compile(r"\bTRUNC\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.date_trunc('day', {_wrap_col(m.group(1))})", Confidence.HIGH),

    # TO_DATE(str, fmt)
    (re.compile(r"\bTO_DATE\s*\(\s*(.+?)\s*,\s*'([^']+)'\s*\)", re.IGNORECASE),
     lambda m: f"F.to_date({_wrap_col(m.group(1))}, '{_pc_date_fmt(m.group(2))}')",
     Confidence.MEDIUM),
    (re.compile(r"\bTO_DATE\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.to_date({_wrap_col(m.group(1))})", Confidence.MEDIUM),

    # TO_CHAR(date, fmt)
    (re.compile(r"\bTO_CHAR\s*\(\s*(.+?)\s*,\s*'([^']+)'\s*\)", re.IGNORECASE),
     lambda m: f"F.date_format({_wrap_col(m.group(1))}, '{_pc_date_fmt(m.group(2))}')",
     Confidence.MEDIUM),

    # ADD_TO_DATE(date, 'DD', n)
    (re.compile(r"\bADD_TO_DATE\s*\(\s*(.+?)\s*,\s*'DD'\s*,\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.date_add({_wrap_col(m.group(1))}, {m.group(2)})", Confidence.HIGH),
    (re.compile(r"\bADD_TO_DATE\s*\(\s*(.+?)\s*,\s*'MM'\s*,\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.add_months({_wrap_col(m.group(1))}, {m.group(2)})", Confidence.HIGH),

    # DATE_DIFF(date1, date2, 'DD')
    (re.compile(r"\bDATE_DIFF\s*\(\s*(.+?)\s*,\s*(.+?)\s*,\s*'DD'\s*\)", re.IGNORECASE | re.DOTALL),
     lambda m: f"F.datediff({_wrap_col(m.group(1))}, {_wrap_col(m.group(2))})",
     Confidence.HIGH),

    # LAST_DAY
    (re.compile(r"\bLAST_DAY\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.last_day({_wrap_col(m.group(1))})", Confidence.HIGH),

    # ISNULL / IS_NULL → col.isNull()
    (re.compile(r"\bISNULL\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"{_wrap_col(m.group(1))}.isNull()", Confidence.HIGH),
    (re.compile(r"\bIS_NULL\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"{_wrap_col(m.group(1))}.isNull()", Confidence.HIGH),

    # ISNUMBER / IS_NUMBER
    (re.compile(r"\bIS_NUMBER\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: (
         f"({_wrap_col(m.group(1))}.cast('double').isNotNull())"
     ), Confidence.MEDIUM),

    # CAST(col AS type)
    (re.compile(r"\bCAST\s*\(\s*(.+?)\s+AS\s+(\w+)\s*\)", re.IGNORECASE),
     lambda m: f"{_wrap_col(m.group(1))}.cast('{m.group(2).lower()}')",
     Confidence.HIGH),

    # TO_INTEGER / TO_BIGINT / TO_FLOAT / TO_DECIMAL / TO_STRING
    (re.compile(r"\bTO_INTEGER\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"{_wrap_col(m.group(1))}.cast('int')", Confidence.HIGH),
    (re.compile(r"\bTO_BIGINT\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"{_wrap_col(m.group(1))}.cast('long')", Confidence.HIGH),
    (re.compile(r"\bTO_FLOAT\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"{_wrap_col(m.group(1))}.cast('double')", Confidence.HIGH),
    (re.compile(r"\bTO_DECIMAL\s*\(\s*(.+?)\s*,\s*\d+\s*,\s*\d+\s*\)", re.IGNORECASE),
     lambda m: f"{_wrap_col(m.group(1))}.cast('decimal')", Confidence.HIGH),
    (re.compile(r"\bTO_STRING\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"{_wrap_col(m.group(1))}.cast('string')", Confidence.HIGH),

    # Aggregation functions (used in Aggregator transformation expressions)
    (re.compile(r"\bSUM\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.sum({_wrap_col(m.group(1))})", Confidence.HIGH),
    (re.compile(r"\bCOUNT\s*\(\s*\*\s*\)", re.IGNORECASE),
     lambda m: "F.count(F.lit(1))", Confidence.HIGH),
    (re.compile(r"\bCOUNT\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.count({_wrap_col(m.group(1))})", Confidence.HIGH),
    (re.compile(r"\bAVG\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.avg({_wrap_col(m.group(1))})", Confidence.HIGH),
    (re.compile(r"\bMIN\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.min({_wrap_col(m.group(1))})", Confidence.HIGH),
    (re.compile(r"\bMAX\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.max({_wrap_col(m.group(1))})", Confidence.HIGH),
    (re.compile(r"\bFIRST\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.first({_wrap_col(m.group(1))})", Confidence.HIGH),
    (re.compile(r"\bLAST\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.last({_wrap_col(m.group(1))})", Confidence.HIGH),
    (re.compile(r"\bMEDIAN\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"F.percentile_approx({_wrap_col(m.group(1))}, 0.5)",
     Confidence.MEDIUM),

    # NEXTVAL (Sequence Generator port references — these become monotonically_increasing_id)
    (re.compile(r"\bNEXTVAL\b", re.IGNORECASE),
     lambda m: "F.monotonically_increasing_id()",
     Confidence.MEDIUM),

    # ERROR / ABORT — no-ops in PySpark context
    (re.compile(r"\bABORT\s*\(\s*(.+?)\s*\)", re.IGNORECASE),
     lambda m: f"# TODO: ABORT({m.group(1)}) — handle error routing manually",
     Confidence.LOW),
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_KNOWN_FUNC_RE = re.compile(r"\b[A-Za-z_][A-Za-z0-9_]*\s*\(")


def _wrap_col(token: str) -> str:
    """Wrap a bare identifier in F.col(); leave literals and already-wrapped alone."""
    t = token.strip()
    if not t:
        return t
    # Already has F. prefix, is a number, or is a quoted string
    if t.startswith("F.") or t.startswith('"') or t.startswith("'"):
        return t
    if re.match(r"^-?\d+(\.\d+)?$", t):
        return t
    if _IDENTIFIER_RE.match(t):
        return f'F.col("{t}")'
    return t


def _pc_date_fmt(fmt: str) -> str:
    """Best-effort PC date format → Java SimpleDateFormat (used by Spark)."""
    mapping = {
        "YYYY": "yyyy", "YY": "yy",
        "MM": "MM", "MON": "MMM", "MONTH": "MMMM",
        "DD": "dd", "DY": "EEE", "DAY": "EEEE",
        "HH24": "HH", "HH12": "hh", "HH": "HH",
        "MI": "mm", "SS": "ss",
        "AM": "a", "PM": "a",
    }
    result = fmt
    for pc, java in sorted(mapping.items(), key=lambda x: -len(x[0])):
        result = result.replace(pc, java)
    return result


def _split_args(s: str) -> List[str]:
    """Split function arguments respecting nested parentheses and quotes."""
    args, current, depth, in_quote = [], [], 0, False
    quote_char = None
    for ch in s:
        if in_quote:
            current.append(ch)
            if ch == quote_char:
                in_quote = False
        elif ch in ('"', "'"):
            in_quote = True
            quote_char = ch
            current.append(ch)
        elif ch == "(":
            depth += 1
            current.append(ch)
        elif ch == ")":
            depth -= 1
            current.append(ch)
        elif ch == "," and depth == 0:
            args.append("".join(current).strip())
            current = []
        else:
            current.append(ch)
    if current:
        args.append("".join(current).strip())
    return args


def _translate_inner(expr: str) -> str:
    """Translate without creating a TranslationResult (used for nested calls)."""
    return translate(expr.strip()).pyspark_expr


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def translate(expr: str) -> TranslationResult:
    """
    Translate a PowerCenter expression string to PySpark.

    Returns a TranslationResult with the translated expression and confidence.
    """
    if not expr or not expr.strip():
        return TranslationResult("None")

    result = TranslationResult(expr.strip())
    log.debug("Translating PC expression (length=%d)", len(expr))

    # 1. Token replacements
    for pattern, replacement in _TOKEN_MAP.items():
        result.pyspark_expr = re.sub(pattern, replacement, result.pyspark_expr, flags=re.IGNORECASE)

    # 2. Function patterns (applied repeatedly until no more changes — handles nesting)
    for _ in range(5):  # max 5 nesting levels
        changed = False
        for regex, repl, confidence in _FUNC_PATTERNS:
            new_expr = regex.sub(
                repl if callable(repl) else repl,
                result.pyspark_expr
            )
            if new_expr != result.pyspark_expr:
                result.pyspark_expr = new_expr
                changed = True
                if confidence != Confidence.HIGH:
                    result.downgrade(confidence, f"Used {confidence} confidence translation")
        if not changed:
            break

    # 3. Wrap remaining bare identifiers that look like column references
    #    (not already wrapped, not inside F.col/F.lit, not a keyword)
    result.pyspark_expr = _wrap_remaining_identifiers(result.pyspark_expr)

    # 4. Detect untranslated PC functions → flag LOW confidence
    _check_unknown_functions(result)

    return result


_PYSPARK_PREFIXED = re.compile(r"\bF\.[a-z_]+\(")
_ALREADY_COL_RE  = re.compile(r'F\.col\("([^"]+)"\)')
_BARE_IDENT_RE   = re.compile(r"(?<!['\".])(?<!\w)\b([A-Z_][A-Z0-9_]{1,})\b(?!\s*\()(?!['\"])")
_PYTHON_KEYWORDS = {
    "True", "False", "None", "and", "or", "not", "in", "is",
    "if", "else", "elif", "for", "while", "return",
}


def _wrap_remaining_identifiers(expr: str) -> str:
    """
    Wrap ALL-CAPS identifiers (likely column names) in F.col() if not already wrapped.
    Skips: Python keywords, numeric literals, already-wrapped tokens.
    """
    def replacer(m: re.Match) -> str:
        token = m.group(1)
        if token in _PYTHON_KEYWORDS:
            return token
        # Check if this token is already inside F.col(...)
        start = m.start()
        preceding = expr[max(0, start - 10):start]
        if 'F.col("' in preceding or "F.lit(" in preceding:
            return token
        return f'F.col("{token}")'

    return _BARE_IDENT_RE.sub(replacer, expr)


_UNKNOWN_FUNC_RE = re.compile(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(")
_KNOWN_TRANSLATED = {
    "F", "col", "lit", "when", "otherwise", "isNull", "isNotNull",
    "isin", "cast", "substring", "length", "upper", "lower",
    "ltrim", "rtrim", "trim", "lpad", "rpad", "concat", "round",
    "abs", "ceil", "floor", "date_trunc", "to_date", "date_format",
    "date_add", "add_months", "datediff", "last_day", "current_timestamp",
    "current_date", "coalesce", "sum", "count", "avg", "min", "max",
    "first", "last", "percentile_approx", "monotonically_increasing_id",
    "date_sub", "int", "long", "double", "decimal", "string",
}


def _check_unknown_functions(result: TranslationResult) -> None:
    for m in _UNKNOWN_FUNC_RE.finditer(result.pyspark_expr):
        fname = m.group(1)
        if fname not in _KNOWN_TRANSLATED and not fname.startswith("F"):
            result.downgrade(
                Confidence.LOW,
                f"Unknown function '{fname}' — verify translation is correct"
            )
            break


# ---------------------------------------------------------------------------
# Filter / condition translation
# ---------------------------------------------------------------------------

def translate_filter(condition: str) -> TranslationResult:
    """
    Translate a PC filter condition to a PySpark filter expression string.
    Wraps the result in df.filter(...) compatible syntax.

    Examples:
      "STATUS != 'CANCELLED'"       → "F.col('STATUS') != 'CANCELLED'"
      "ORDER_AMT > 0 AND STATUS = 'A'" → "(F.col('ORDER_AMT') > 0) & (F.col('STATUS') == 'A')"
    """
    if not condition or not condition.strip():
        return TranslationResult("True")

    cond = condition.strip()

    # Normalize SQL AND/OR to Python & / |
    cond = re.sub(r"\bAND\b", "&", cond, flags=re.IGNORECASE)
    cond = re.sub(r"\bOR\b",  "|", cond, flags=re.IGNORECASE)
    cond = re.sub(r"\bNOT\b", "~", cond, flags=re.IGNORECASE)
    # SQL = to == (but not !=, >=, <=)
    cond = re.sub(r"(?<![!<>])=(?!=)", "==", cond)

    result = translate(cond)

    # Wrap compound conditions in parens for safety
    if "&" in result.pyspark_expr or "|" in result.pyspark_expr:
        # Wrap each clause
        parts = re.split(r"(\s*[&|]\s*)", result.pyspark_expr)
        wrapped = []
        for part in parts:
            if part.strip() in ("&", "|", "&", "|"):
                wrapped.append(f" {part.strip()} ")
            elif part.strip():
                wrapped.append(f"({part.strip()})")
        result.pyspark_expr = "".join(wrapped)

    return result


def translate_join_condition(condition: str) -> TranslationResult:
    """
    Translate a PC join condition to a PySpark join expression.
    PC: "ORDER_ID = ORDER_ID" → list of join column names for simple equi-joins,
    or a full expression for complex conditions.
    """
    if not condition or not condition.strip():
        return TranslationResult("[]  # TODO: add join condition")

    # Simple equi-join: COL1 = COL2
    equi = re.match(r"^\s*(\w+)\s*=\s*(\w+)\s*$", condition.strip())
    if equi:
        left, right = equi.group(1), equi.group(2)
        if left == right:
            return TranslationResult(f'"{left}"')
        # Different column names — use explicit expression
        return TranslationResult(
            f'F.col("left.{left}") == F.col("right.{right}")',
            notes=["Renamed join columns — verify table aliases are correct"]
        )

    # Multi-column equi-join: A = A AND B = B
    pairs = re.findall(r"(\w+)\s*=\s*(\w+)", condition)
    if pairs and all(l == r for l, r in pairs):
        cols = [f'"{l}"' for l, _ in pairs]
        return TranslationResult(f"[{', '.join(cols)}]")

    # Complex — fall back to full translation
    result = translate_filter(condition)
    result.downgrade(Confidence.MEDIUM, "Complex join condition — verify aliases")
    return result
