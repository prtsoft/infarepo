"""
Databricks Python notebook builder.

Generates .py-format Databricks notebooks (cells separated by
# COMMAND ----------) from a MappingDef.

HIPAA note: generated notebooks never log actual data values — only
row counts, table names, and timestamps.
"""
from __future__ import annotations

import sys
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from pc_extractor.models import (
    MappingDef,
    SourceDef,
    TargetDef,
    TransformationDef,
    TransformationType,
)
from .models import DatabricksNotebook, NotebookCell

# Try to import expr_translator from glue_gen
try:
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from glue_gen.expr_translator import translate, translate_filter
    _HAS_EXPR_TRANSLATOR = True
except ImportError:
    _HAS_EXPR_TRANSLATOR = False


# ---------------------------------------------------------------------------
# PHI column name heuristics (HIPAA)
# ---------------------------------------------------------------------------

_PHI_PATTERNS = re.compile(
    r"(patient|member|ssn|dob|birth|name|address|phone|email|mrn|npi|"
    r"diagnosis|medication|insurance|claim|provider|discharge|admit)",
    re.IGNORECASE,
)


def _has_phi_columns(mapping: MappingDef) -> bool:
    """Return True if any target field name looks like PHI."""
    for t in mapping.transformations:
        for port in t.ports:
            if _PHI_PATTERNS.search(port.name):
                return True
    # Also check source fields embedded in instances list (names from mapping)
    return False


def _phi_in_field_names(field_names: List[str]) -> bool:
    return any(_PHI_PATTERNS.search(f) for f in field_names)


def _safe_var(name: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", name.lower()).strip("_")


# ---------------------------------------------------------------------------
# Expression translation helper
# ---------------------------------------------------------------------------

def _translate_expr(expr: str) -> Tuple[str, bool]:
    """
    Translate a PC expression to PySpark.
    Returns (translated_expr, was_translated).
    If expr_translator unavailable, returns raw expr as comment.
    """
    if not expr or not expr.strip():
        return "None", True
    if _HAS_EXPR_TRANSLATOR:
        result = translate(expr)
        return result.pyspark_expr, True
    else:
        return f"# PC expr (translate manually): {expr}", False


def _translate_filter_expr(expr: str) -> Tuple[str, bool]:
    if not expr or not expr.strip():
        return "True", True
    if _HAS_EXPR_TRANSLATOR:
        result = translate_filter(expr)
        return result.pyspark_expr, True
    else:
        return f"# PC filter (translate manually): {expr}", False


# ---------------------------------------------------------------------------
# Source read code generation
# ---------------------------------------------------------------------------

def _source_read_code(src_name: str, db_type: str, table_name: str,
                      sql_override: Optional[str],
                      source_fields: Optional[list] = None) -> List[str]:
    """
    Generate PySpark code lines to read a source DataFrame.

    Parameters
    ----------
    source_fields:
        Optional list of FieldDef objects from the folder's SourceDef.  Used
        to generate accurate F.substring() column extraction for FIXED_WIDTH sources.
    """
    db_upper = db_type.upper() if db_type else ""
    lines: List[str] = []
    var = _safe_var(src_name)

    if db_upper in ("SQLSERVER", "MICROSOFT SQL SERVER", "SQL SERVER"):
        table_or_query = sql_override if sql_override else table_name
        # Use sub-query wrapping for sql_override
        if sql_override:
            dbtable_expr = f"({sql_override}) AS sq_{var}"
        else:
            dbtable_expr = table_name
        lines += [
            f"# Source: {src_name} ({db_type})",
            f'jdbc_url_{var} = (',
            f'    f"jdbc:sqlserver://{{dbutils.secrets.get(\'migration\', \'sql_host\')}}"',
            f'    f";databaseName={{dbutils.secrets.get(\'migration\', \'sql_db\')}}"',
            f')',
            f'df_{var} = (',
            f'    spark.read.format("jdbc")',
            f'    .option("url", jdbc_url_{var})',
            f'    .option("dbtable", "{dbtable_expr}")',
            f'    .option("user", dbutils.secrets.get("migration", "sql_user"))',
            f'    .option("password", dbutils.secrets.get("migration", "sql_password"))',
            f'    .load()',
            f')',
        ]

    elif db_upper in ("ORACLE",):
        table_or_query = sql_override if sql_override else table_name
        if sql_override:
            dbtable_expr = f"({sql_override}) sq_{var}"
        else:
            dbtable_expr = table_name
        lines += [
            f"# Source: {src_name} ({db_type})",
            f'jdbc_url_{var} = (',
            f'    f"jdbc:oracle:thin:@{{dbutils.secrets.get(\'migration\', \'oracle_host\')}}"',
            f'    f":{{dbutils.secrets.get(\'migration\', \'oracle_port\')}}"',
            f'    f"/{{dbutils.secrets.get(\'migration\', \'oracle_db\')}}"',
            f')',
            f'df_{var} = (',
            f'    spark.read.format("jdbc")',
            f'    .option("url", jdbc_url_{var})',
            f'    .option("dbtable", "{dbtable_expr}")',
            f'    .option("user", dbutils.secrets.get("migration", "oracle_user"))',
            f'    .option("password", dbutils.secrets.get("migration", "oracle_password"))',
            f'    .load()',
            f')',
        ]

    elif db_upper in ("S3_CSV", "FLAT FILE", "FILE"):
        s3_path_param = f"s3_input_path_{var}"
        lines += [
            f"# Source: {src_name} ({db_type}) — S3 CSV",
            f'{s3_path_param} = params.get("s3_input_path", "s3://BUCKET/PATH/")  # TODO: set correct S3 path',
            f'df_{var} = (',
            f'    spark.read.format("csv")',
            f'    .option("header", "true")',
            f'    .option("inferSchema", "true")',
            f'    .load({s3_path_param})',
            f')',
        ]

    elif db_upper in ("FIXED_WIDTH",):
        s3_path_param = f"s3_input_path_{var}"
        lines += [
            f"# Source: {src_name} ({db_type}) — Fixed-Width file",
            f'{s3_path_param} = params.get("s3_input_path", "s3://BUCKET/PATH/")  # TODO: set correct S3 path',
            f'df_{var}_raw = (',
            f'    spark.read.format("text")',
            f'    .load({s3_path_param})',
            f')',
        ]
        if source_fields:
            # Generate F.substring extractions from field widths
            # Import here to avoid circular dependency at module level
            try:
                from pc_extractor.datatypes import map_type
            except ImportError:
                map_type = None

            lines.append(f"# Extract fixed-width columns using field positions from PC metadata")
            start = 1
            select_parts = []
            for fld in source_fields:
                width = fld.length or fld.precision or 1
                spark_type = map_type(fld.datatype, fld.precision, fld.scale) if map_type else "string"
                select_parts.append(
                    f'    F.substring("value", {start}, {width}).cast("{spark_type}").alias("{fld.name}")'
                )
                start += width
            lines.append(f'df_{var} = df_{var}_raw.select(')
            lines.extend(f'{p},' for p in select_parts)
            lines.append(f')')
        else:
            lines += [
                f'# TODO: extract fixed-width columns using F.substring()',
                f'# Example: F.substring("value", 1, 10).alias("field_name")',
                f'df_{var} = df_{var}_raw  # TODO: add F.substring column extractions',
            ]

    elif db_upper in ("EXCEL",):
        s3_path_param = f"s3_input_path_{var}"
        lines += [
            f"# Source: {src_name} ({db_type}) — Excel",
            f'# TODO: install com.crealytics:spark-excel library on cluster',
            f'{s3_path_param} = params.get("s3_input_path", "s3://BUCKET/PATH/")  # TODO: set correct S3 path',
            f'df_{var} = (',
            f'    spark.read.format("com.crealytics.spark.excel")',
            f'    .option("useHeader", "true")',
            f'    .load({s3_path_param})',
            f')',
        ]

    elif db_upper in ("DELTA", "DATABRICKS", "S3_DELTA"):
        delta_table = table_name or src_name
        lines += [
            f"# Source: {src_name} ({db_type}) — Delta table",
            f'df_{var} = spark.table("{delta_table}")',
        ]

    elif db_upper in ("S3_PARQUET",):
        s3_path_param = f"s3_input_path_{var}"
        lines += [
            f"# Source: {src_name} ({db_type}) — S3 Parquet",
            f'{s3_path_param} = params.get("s3_input_path", "s3://BUCKET/PATH/")  # TODO: set correct S3 path',
            f'df_{var} = spark.read.format("parquet").load({s3_path_param})',
        ]

    else:
        lines += [
            f"# Source: {src_name} ({db_type}) — UNKNOWN source type, manual implementation required",
            f'# TODO: implement read for {db_type}',
            f'df_{var} = None  # TODO',
        ]

    return lines


# ---------------------------------------------------------------------------
# Transformation code generation
# ---------------------------------------------------------------------------

def _transformation_code(
    t: TransformationDef,
    input_var: str,
    warnings: List[str],
    sp_strategy: str = "databricks-call",
) -> Tuple[List[str], str]:
    """
    Generate code lines for a single transformation.
    Returns (lines, output_var_name).

    sp_strategy: how to handle STORED_PROCEDURE transformations.
      'databricks-call'  — spark.sql("CALL schema.proc_name(...)")
      'pyspark-udf'      — @udf skeleton
      'jdbc-passthrough' — spark.read.jdbc(..., query="EXEC proc_name")
    """
    out_var = _safe_var(t.name)
    ttype = t.type
    lines: List[str] = [f"# Transformation: {t.name} ({ttype.value})"]

    if ttype == TransformationType.SOURCE_QUALIFIER:
        # SOURCE_QUALIFIER with sql_query override handled in source read cell;
        # here we just alias the input DataFrame
        lines.append(f"df_{out_var} = df_{input_var}  # SOURCE_QUALIFIER passthrough")

    elif ttype == TransformationType.EXPRESSION:
        lines.append(f"df_{out_var} = df_{input_var}")
        # expressions from ports
        for port in t.ports:
            if port.expression and port.expression.strip():
                translated, ok = _translate_expr(port.expression)
                if ok:
                    lines.append(f'df_{out_var} = df_{out_var}.withColumn("{port.name}", {translated})')
                else:
                    lines.append(f'# {translated}')
                    lines.append(f'# df_{out_var} = df_{out_var}.withColumn("{port.name}", ...)  # TODO')
                    warnings.append(f"Expression '{port.expression}' for port '{port.name}' needs manual translation.")

    elif ttype == TransformationType.FILTER:
        cond = t.filter_condition or ""
        translated, ok = _translate_filter_expr(cond)
        if ok:
            lines.append(f'df_{out_var} = df_{input_var}.filter(F.expr("{translated}"))')
        else:
            lines.append(f'# {translated}')
            lines.append(f'df_{out_var} = df_{input_var}  # TODO: apply filter manually')
            warnings.append(f"Filter condition '{cond}' needs manual translation.")

    elif ttype == TransformationType.AGGREGATOR:
        # Group-by ports (with port_type INPUT) vs agg ports
        group_cols = [p.name for p in t.ports if p.port_type in ("", "INPUT", "INPUT/OUTPUT") and not p.expression]
        agg_exprs = []
        for port in t.ports:
            if port.expression and port.expression.strip():
                translated, _ = _translate_expr(port.expression)
                agg_exprs.append(f'{translated}.alias("{port.name}")')
        if group_cols:
            group_str = ", ".join(f'"{c}"' for c in group_cols)
            lines.append(f'df_{out_var} = df_{input_var}.groupBy({group_str})')
        else:
            lines.append(f'df_{out_var} = df_{input_var}.groupBy()')
        if agg_exprs:
            agg_str = ",\n        ".join(agg_exprs)
            lines.append(f'df_{out_var} = df_{out_var}.agg(')
            lines.append(f'    {agg_str}')
            lines.append(f')')
        else:
            lines.append(f'# TODO: define aggregation expressions')
            warnings.append(f"Aggregator '{t.name}' has no expression ports — define agg() manually.")

    elif ttype == TransformationType.JOINER:
        cond = t.join_condition or ""
        jtype = (t.join_type or "").lower()
        spark_how = "inner"
        if "outer" in jtype:
            if "master" in jtype or "left" in jtype:
                spark_how = "left"
            elif "detail" in jtype or "right" in jtype:
                spark_how = "right"
            elif "full" in jtype:
                spark_how = "outer"
        if cond:
            translated, ok = _translate_expr(cond)
            lines += [
                f'# TODO: replace df_left / df_right with actual DataFrames',
                f'df_{out_var} = df_left.join(df_right, {translated}, how="{spark_how}")',
            ]
        else:
            lines += [
                f'# TODO: define join condition',
                f'df_{out_var} = df_left.join(df_right, "UNKNOWN_JOIN_KEY", how="{spark_how}")  # TODO',
            ]
        warnings.append(f"Joiner '{t.name}': verify df_left/df_right variable names and join key.")

    elif ttype == TransformationType.LOOKUP:
        cond = t.lookup_condition or ""
        lkp_table = t.attributes.get("Lookup Table Name", "") or t.attributes.get("Lookup table name", "")
        sql_override = t.attributes.get("Lookup Sql Override", "") or t.attributes.get("Lookup SQL Override", "")
        lkp_var = f"lookup_df_{out_var}"
        lines += [
            f'# LOOKUP — broadcast join on {lkp_table or "lookup table"}',
        ]
        if sql_override:
            lines += [
                f'# Lookup SQL override: {sql_override}',
                f'{lkp_var} = spark.read.format("jdbc")  # TODO: configure JDBC options + SQL override',
            ]
        elif lkp_table:
            lines += [
                f'{lkp_var} = spark.table("{lkp_table}")  # or spark.read.jdbc(url, "{lkp_table}")',
            ]
        else:
            lines += [
                f'# TODO: define {lkp_var} — lookup table not specified in PC metadata',
                f'{lkp_var} = None  # TODO',
            ]
            warnings.append(f"Lookup '{t.name}': lookup table name not found — define {lkp_var} manually.")
        lines += [
            f'df_{out_var} = df_{input_var}.join(F.broadcast({lkp_var}), {repr(cond) if cond else "[]"}, how="left")',
        ]

    elif ttype == TransformationType.UPDATE_STRATEGY:
        strategy_expr = t.attributes.get("Update Strategy Expression", "DD_INSERT")
        lines += [
            f'# UPDATE STRATEGY — PC expression: {strategy_expr}',
            f'# DD_INSERT=0, DD_UPDATE=1, DD_DELETE=2, DD_REJECT=3',
            f'df_{out_var} = df_{input_var}.withColumn("_update_flag", F.lit(0))  # TODO: translate strategy expr',
            f'',
            f'# Split by operation — wire each to its target write below',
            f'df_{out_var}_insert = df_{out_var}.filter(F.col("_update_flag") == 0)',
            f'df_{out_var}_update = df_{out_var}.filter(F.col("_update_flag") == 1)',
            f'df_{out_var}_delete = df_{out_var}.filter(F.col("_update_flag") == 2)',
            f'df_{out_var}_reject = df_{out_var}.filter(F.col("_update_flag") == 3)',
        ]
        warnings.append(
            f"Update Strategy '{t.name}': translate strategy expression '{strategy_expr}' "
            "and wire each split DataFrame to the correct Delta write."
        )

    elif ttype == TransformationType.STORED_PROCEDURE:
        sp_name = t.stored_proc_name or t.name
        if sp_strategy == "databricks-call":
            lines += [
                f'# Stored Procedure: {sp_name}',
                f'# Strategy: databricks-call (Unity Catalog CALL statement)',
                f'spark.sql("CALL {sp_name}()")',
                f'# TODO: add actual parameters to CALL above',
                f'df_{out_var} = df_{input_var}  # passthrough after procedure call',
            ]
        elif sp_strategy == "pyspark-udf":
            safe_sp = _safe_var(sp_name)
            lines += [
                f'# Stored Procedure: {sp_name}',
                f'# Strategy: pyspark-udf skeleton',
                f'@F.udf(returnType="string")  # TODO: update return type',
                f'def {safe_sp}_udf(*args):',
                f'    # TODO: implement logic from stored procedure {sp_name}',
                f'    raise NotImplementedError("Implement {sp_name}")',
                f'df_{out_var} = df_{input_var}  # TODO: apply {safe_sp}_udf to relevant columns',
            ]
        else:  # jdbc-passthrough
            lines += [
                f'# Stored Procedure: {sp_name}',
                f'# Strategy: jdbc-passthrough',
                f'df_{out_var} = spark.read.jdbc(',
                f'    url=src_jdbc_url,',
                f'    table="(EXEC {sp_name}) AS sp_result",  # TODO: add parameters',
                f'    properties={{"user": src_user, "password": src_password}}',
                f')',
            ]
        warnings.append(
            f"Stored procedure '{sp_name}': review and test the generated {sp_strategy} code."
        )

    else:
        lines += [
            f'# TODO: transformation type "{ttype.value}" not auto-generated',
            f'df_{out_var} = df_{input_var}  # TODO: implement {t.name}',
        ]
        warnings.append(f"Transformation '{t.name}' of type '{ttype.value}' requires manual implementation.")

    return lines, out_var


# ---------------------------------------------------------------------------
# Merge key detection
# ---------------------------------------------------------------------------

# Patterns that suggest a primary key column name
_PK_PATTERNS = re.compile(
    r"\b(id|key|pk|surrogate_key|rowguid|guid|uuid)\b",
    re.IGNORECASE,
)


def _detect_merge_key(target_fields: Optional[list]) -> list:
    """
    Auto-detect merge key columns from TargetDef.fields.

    Priority:
    1. Fields with key_type == "PRIMARY KEY"
    2. Fields whose name ends in _ID or _KEY (word boundary)
    3. First field named ID exactly
    4. Empty list (caller should emit TODO)
    """
    if not target_fields:
        return []

    # Priority 1: explicit PRIMARY KEY
    pk_fields = [f.name for f in target_fields if f.key_type.upper() == "PRIMARY KEY"]
    if pk_fields:
        return pk_fields

    # Priority 2: name heuristic
    heuristic = [
        f.name for f in target_fields
        if re.search(r"(_id|_key|_pk)$", f.name, re.IGNORECASE)
    ]
    if heuristic:
        return heuristic[:1]  # Use first match only

    # Priority 3: exact "ID"
    for fld in target_fields:
        if fld.name.upper() == "ID":
            return [fld.name]

    return []


# ---------------------------------------------------------------------------
# Write cell generation
# ---------------------------------------------------------------------------

def _write_cell_code(target_name: str, db_type: str, table_name: str,
                     load_type: str, df_var: str,
                     target_fields: Optional[list] = None) -> List[str]:
    """
    Generate Delta write code based on load_type.

    Parameters
    ----------
    target_fields:
        Optional list of FieldDef objects from the folder's TargetDef.  Used
        to auto-detect the merge key column(s) for upsert mode.
    """
    lines: List[str] = [f"# Target: {target_name} ({db_type}) — load_type: {load_type}"]
    lt = (load_type or "insert").lower()
    tbl = table_name or target_name

    if lt == "upsert":
        # Auto-detect merge key from target field definitions
        merge_key = _detect_merge_key(target_fields)
        if merge_key:
            merge_cond = " AND ".join(f"t.{k} = s.{k}" for k in merge_key)
            key_comment = f"# Merge key auto-detected from PRIMARY KEY fields: {merge_key}"
        else:
            merge_cond = "t.id_column = s.id_column  -- TODO: set correct merge key"
            key_comment = '# TODO: replace "id_column" with actual merge key column(s)'

        lines += [
            f'target_table = f"{tbl}"',
            key_comment,
            f'merge_condition = "{merge_cond}"',
            f'',
            f'DeltaTable.forName(spark, target_table).alias("t").merge(',
            f'    df_final.alias("s"),',
            f'    merge_condition,',
            f').whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()',
        ]
    elif lt in ("overwrite", "truncate_insert"):
        lines += [
            f'target_table = f"{tbl}"',
            f'(',
            f'    df_final.write',
            f'    .format("delta")',
            f'    .mode("overwrite")',
            f'    .saveAsTable(target_table)',
            f')',
        ]
    else:
        # insert / append
        lines += [
            f'target_table = f"{tbl}"',
            f'(',
            f'    df_final.write',
            f'    .format("delta")',
            f'    .mode("append")',
            f'    .saveAsTable(target_table)',
            f')',
        ]
    return lines


# ---------------------------------------------------------------------------
# Main builder class
# ---------------------------------------------------------------------------

class DatabricksNotebookBuilder:
    """
    Builds a Databricks Python notebook (.py format) from a MappingDef.
    """

    def build(
        self,
        mapping: MappingDef,
        folder_sources: Optional[dict] = None,
        folder_targets: Optional[dict] = None,
        session_params: Optional[dict] = None,
        sp_strategy: str = "databricks-call",
    ) -> DatabricksNotebook:
        """
        Build a Databricks notebook from a MappingDef.

        Parameters
        ----------
        folder_sources:
            Dict[name, SourceDef] from the same folder.  Provides field-width
            metadata for FIXED_WIDTH source column extraction.
        folder_targets:
            Dict[name, TargetDef] from the same folder.  Provides key field
            metadata for Delta merge key auto-detection.
        session_params:
            Flat dict from param-translator glue-params JSON.
            Format: { "PARAM_NAME": {"value": ..., "type": ..., "spark_value": ...} }
            When set, actual values replace empty-string defaults in widget declarations.
        """
        warnings: List[str] = []
        cells: List[NotebookCell] = []

        # Collect field names for PHI check
        all_field_names: List[str] = []
        for t in mapping.transformations:
            all_field_names += [p.name for p in t.ports]

        has_phi = _phi_in_field_names(all_field_names)

        # 1. Header cell (markdown)
        cells.append(self._header_cell(mapping, has_phi))

        # 2. Imports cell
        cells.append(self._imports_cell())

        # 3. Parameters cell
        cells.append(self._params_cell(mapping, session_params=session_params))

        # 4. Source read cell
        src_cell_lines, src_vars = self._source_read_cell(
            mapping, warnings, folder_sources=folder_sources or {}
        )
        cells.append(NotebookCell("code", "\n".join(src_cell_lines)))

        # 5. Transform cell
        transform_lines, final_var, t_warnings = self._transform_cell(mapping, src_vars, sp_strategy=sp_strategy)
        warnings += t_warnings
        cells.append(NotebookCell("code", "\n".join(transform_lines)))

        # 6. Write cell
        write_lines = self._write_cell(mapping, final_var, warnings, folder_targets=folder_targets or {})
        cells.append(NotebookCell("code", "\n".join(write_lines)))

        # 7. Summary cell
        cells.append(self._summary_cell(mapping))

        if has_phi:
            warnings.append("PHI-like column names detected — verify HIPAA compliance before deploying.")

        return DatabricksNotebook(
            mapping_name=mapping.name,
            folder=mapping.folder,
            cells=cells,
            warnings=warnings,
        )

    # ------------------------------------------------------------------
    # Cell builders
    # ------------------------------------------------------------------

    def _header_cell(self, mapping: MappingDef, has_phi: bool) -> NotebookCell:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        source_systems = ", ".join(
            t.name for t in mapping.transformations
            if t.type == TransformationType.SOURCE_QUALIFIER
        ) or "N/A"
        lines = [
            f"# {mapping.name}",
            f"",
            f"**Folder:** {mapping.folder}  ",
            f"**Generated:** {now}  ",
            f"**Migrated from:** Informatica PowerCenter 10.5  ",
            f"**Source system(s):** {source_systems}  ",
        ]
        if has_phi:
            lines += [
                f"",
                f"**HIPAA NOTICE:** This notebook processes PHI-sensitive columns.  ",
                f"Do not log, print, or display actual data values in production.  ",
                f"Ensure cluster is in a HIPAA-compliant workspace with audit logging enabled.  ",
            ]
        return NotebookCell("markdown", "\n".join(lines))

    def _imports_cell(self) -> NotebookCell:
        code = (
            "from pyspark.sql import functions as F\n"
            "from pyspark.sql.types import *\n"
            "from delta.tables import DeltaTable\n"
        )
        return NotebookCell("code", code.rstrip())

    def _params_cell(
        self,
        mapping: MappingDef,
        session_params: Optional[dict] = None,
    ) -> NotebookCell:
        sp = session_params or {}
        lines = [
            'dbutils.widgets.text("env", "dev")',
            'dbutils.widgets.text("catalog", "main")',
            'dbutils.widgets.text("schema", "default")',
        ]
        param_keys = ["env", "catalog", "schema"]

        # Add any mapping variables that are parameters
        # Use actual values from session_params when available
        already_added: set = set()
        for var in mapping.variables:
            if var.is_param:
                safe_key = var.name.lstrip("$").lower()
                upper_key = safe_key.upper()
                if upper_key in sp:
                    pdata = sp[upper_key]
                    raw = pdata.get("spark_value") or pdata.get("value") or ""
                    ptype = pdata.get("type", "")
                    # Strip surrounding quotes from spark_value for DATE types
                    default = str(raw).strip('"')
                    comment = f"  # {ptype}" if ptype else ""
                    lines.append(f'dbutils.widgets.text("{safe_key}", "{default}"){comment}')
                else:
                    default = var.default_value or ""
                    lines.append(f'dbutils.widgets.text("{safe_key}", "{default}")')
                param_keys.append(safe_key)
                already_added.add(upper_key)

        # Add any session params not covered by mapping.variables
        for param_name, pdata in sp.items():
            if param_name not in already_added:
                safe_key = param_name.lower()
                raw = pdata.get("spark_value") or pdata.get("value") or ""
                ptype = pdata.get("type", "")
                default = str(raw).strip('"')
                comment = f"  # {ptype}" if ptype else ""
                lines.append(f'dbutils.widgets.text("{safe_key}", "{default}"){comment}')
                param_keys.append(safe_key)

        keys_str = ", ".join(f'"{k}"' for k in param_keys)
        lines.append(f'params = {{k: dbutils.widgets.get(k) for k in [{keys_str}]}}')
        return NotebookCell("code", "\n".join(lines))

    def _source_read_cell(
        self, mapping: MappingDef, warnings: List[str],
        folder_sources: Optional[dict] = None,
    ) -> Tuple[List[str], List[str]]:
        """Returns (lines, list_of_df_var_names)."""
        lines: List[str] = []
        src_vars: List[str] = []

        # Find SOURCE_QUALIFIER transformations
        sq_transforms = [
            t for t in mapping.transformations
            if t.type == TransformationType.SOURCE_QUALIFIER
        ]

        if sq_transforms:
            for t in sq_transforms:
                var = _safe_var(t.name)
                src_vars.append(var)
                sql_q = t.sql_query or ""
                # Try to infer db_type from mapping instances (use first source in mapping)
                db_type = "SQLSERVER"  # default
                table_name = t.name

                # Check if instances list has source info
                for inst in mapping.instances:
                    if inst.name == t.name or inst.transformation_name == t.name:
                        db_type = inst.transformation_type or db_type
                        break

                # Look up source fields for FIXED_WIDTH extraction
                src_def = (folder_sources or {}).get(t.name)
                src_fields = src_def.fields if src_def else None
                src_lines = _source_read_code(t.name, db_type, table_name, sql_q, source_fields=src_fields)
                lines += src_lines
                lines.append("")
        else:
            # No SOURCE_QUALIFIER — fall back: one entry per instance name (source)
            for src_name in (mapping.sources or []):
                var = _safe_var(src_name)
                src_vars.append(var)
                lines += [
                    f"# Source: {src_name} — db_type unknown, manual implementation required",
                    f"# TODO: implement read for source '{src_name}'",
                    f"df_{var} = None  # TODO",
                    "",
                ]
                warnings.append(f"Source '{src_name}' has no SOURCE_QUALIFIER — implement read manually.")

        if not src_vars:
            src_vars = ["src"]
            lines += ["# No sources detected", "df_src = None  # TODO"]

        return lines, src_vars

    def _transform_cell(
        self,
        mapping: MappingDef,
        src_vars: List[str],
        sp_strategy: str = "databricks-call",
    ) -> Tuple[List[str], str, List[str]]:
        """Returns (lines, final_df_var, warnings)."""
        warnings: List[str] = []
        lines: List[str] = []
        current_var = src_vars[0] if src_vars else "src"

        non_sq = [
            t for t in mapping.transformations
            if t.type != TransformationType.SOURCE_QUALIFIER
        ]

        if not non_sq:
            lines.append(f"# No transformations to apply")
            lines.append(f"df_final = df_{current_var}")
            return lines, "final", warnings

        for t in non_sq:
            t_lines, out_var = _transformation_code(t, current_var, warnings, sp_strategy=sp_strategy)
            lines += t_lines
            lines.append("")
            current_var = out_var

        lines.append(f"df_final = df_{current_var}")
        return lines, "final", warnings

    def _write_cell(
        self, mapping: MappingDef, df_var: str, warnings: List[str],
        folder_targets: Optional[dict] = None,
    ) -> List[str]:
        lines: List[str] = []
        if not mapping.targets:
            lines += [
                "# No target definitions found",
                "# TODO: implement write",
            ]
            warnings.append("No target definitions found in mapping.")
            return lines

        # For now handle first target (multiple targets → TODO)
        if len(mapping.targets) > 1:
            warnings.append("Multiple targets detected — only first target is generated. Implement remaining targets manually.")

        tgt_name = mapping.targets[0]

        # Look for TargetDef in instances (mapping.targets contains instance names in real model)
        # In the real MappingDef the targets list contains instance names (str)
        # We write to a delta table named after the target
        # Find TransformationDef for this target if it's an OUTPUT type
        tgt_table = tgt_name
        load_type = "insert"
        db_type = "DELTA"

        # Look for OUTPUT transformation matching this target name
        for t in mapping.transformations:
            if t.type == TransformationType.OUTPUT and t.name == tgt_name:
                break

        # Look up target fields for merge key auto-detection
        tgt_def = (folder_targets or {}).get(tgt_name)
        tgt_fields = tgt_def.fields if tgt_def else None
        write_lines = _write_cell_code(tgt_name, db_type, tgt_table, load_type, df_var, target_fields=tgt_fields)
        lines += write_lines
        return lines

    def _summary_cell(self, mapping: MappingDef) -> NotebookCell:
        target = mapping.targets[0] if mapping.targets else "unknown_target"
        code = (
            "# HIPAA: no PII logged\n"
            f'print(f"Loaded {{df_final.count()}} rows into {target}")\n'
            f'print(f"Completed at: {{__import__(\'datetime\').datetime.now().__import__(\'datetime\').timezone.utc}}")'
        )
        # Simpler version:
        code = (
            "# HIPAA: no PII logged\n"
            f'print(f"Loaded {{df_final.count()}} rows into \\"{target}\\"")'
        )
        return NotebookCell("code", code)


# ---------------------------------------------------------------------------
# Notebook serializer
# ---------------------------------------------------------------------------

_CELL_SEP = "# COMMAND ----------"


def render_notebook(notebook: DatabricksNotebook) -> str:
    """Render a DatabricksNotebook to .py format string."""
    parts: List[str] = []
    for i, cell in enumerate(notebook.cells):
        if cell.cell_type == "markdown":
            # Databricks markdown cell: # MAGIC %md\n# MAGIC <content>
            md_lines = ["# MAGIC %md"]
            for line in cell.source.splitlines():
                md_lines.append(f"# MAGIC {line}" if line else "# MAGIC ")
            parts.append("\n".join(md_lines))
        else:
            parts.append(cell.source)
    return f"\n{_CELL_SEP}\n".join(parts) + "\n"
